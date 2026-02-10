/**
 * Cluster web worker for crime map.
 *
 * Loads crime incident data **on demand** from a FlatGeobuf file using HTTP
 * range requests with spatial filtering. Only features within a padded
 * viewport bbox are fetched, keeping memory usage proportional to the
 * visible area rather than the entire dataset.
 *
 * A 1.5x padding factor is applied to the viewport bbox so that small pans
 * are served from the in-memory cache without any network requests. The
 * cache is invalidated when the viewport moves outside the padded region
 * or the zoom level changes significantly.
 *
 * Performance characteristics:
 * - Viewport load (cache miss): O(k) FlatGeobuf spatial query + O(k log k) Supercluster build
 * - Pan/zoom within cache: O(k) Supercluster query (~instant)
 * - Filter change: O(n log n) Supercluster rebuild from cached points (no network)
 * - Sidebar bbox query: O(log n + k) via rbush
 */

import Supercluster from "supercluster";
import * as flatgeobuf from "flatgeobuf/lib/mjs/geojson.js";
import RBush from "rbush";
import { buildIncidentPredicate } from "../filters/predicates.ts";
import type {
  CrimePoint,
  SidebarFeature,
  BBox,
  WorkerRequest,
  WorkerResponse,
} from "./types.ts";
import type { FilterState } from "../types.ts";

// -- Configuration --

/** Padding factor applied to the viewport bbox for cache locality. */
const BBOX_PADDING_FACTOR = 1.5;

/**
 * Maximum zoom delta before the cache is invalidated. If the user zooms
 * more than this many levels from the cached zoom, a reload is triggered
 * because the spatial density changes significantly.
 */
const MAX_ZOOM_DELTA = 1;

/** How often to send progress updates during a viewport load. */
const PROGRESS_INTERVAL = 10_000;

// Supercluster config: clusters visible at zoom 8-11 (CLUSTER_MAX_ZOOM = 12)
const SUPERCLUSTER_MAX_ZOOM = 11;
const SUPERCLUSTER_RADIUS = 60;

// -- State --

/** Base URL for the FlatGeobuf file, set by the `init` message. */
let baseUrl = "";

/** Points loaded for the current padded viewport region. */
let cachedPoints: CrimePoint[] = [];

/** Pre-built GeoJSON features for cached points. */
let cachedFeatures: GeoJSON.Feature<GeoJSON.Point, CrimePoint>[] = [];

/** The padded bbox that was loaded (used for cache hit checks). */
let cachedBbox: BBox | null = null;

/** The zoom level at which the cache was built. */
let cachedZoom: number | null = null;

/** Whether a spatial load has completed and indexes are ready. */
let dataReady = false;

/** Filtered subset of cachedPoints matching current filters. */
let filteredPoints: CrimePoint[] = [];

/** Current filter state from the main thread. */
let currentFilters: FilterState | null = null;

/** Supercluster index for mid-zoom clustering (zoom 8-11). */
let clusterIndex: Supercluster | null = null;

/** rbush R-tree for fast sidebar bbox queries on filtered points. */
interface RBushItem {
  minX: number;
  minY: number;
  maxX: number;
  maxY: number;
  /** Index into filteredPoints. */
  idx: number;
}

let sidebarTree: RBush<RBushItem> = new RBush<RBushItem>();

/** Sidebar features for the current viewport, cached for pagination. */
let viewportSidebarCache: SidebarFeature[] = [];

/** Latest viewport request sequence number (for cancellation). */
let latestViewportSeq = -1;

/**
 * Sequence number for spatial data loads. Incremented each time a new
 * load begins so that stale in-flight loads can be detected and discarded.
 */
let loadSeq = 0;

/** The last viewport bbox/zoom requested, used for re-querying after filter changes. */
let lastRequestedBbox: BBox | null = null;
let lastRequestedZoom: number | null = null;

// -- Helpers --

function pointToSidebarFeature(p: CrimePoint): SidebarFeature {
  return {
    id: p.id,
    sid: p.sid,
    subcategory: p.subcategory,
    category: p.category,
    severity: p.severity,
    city: p.city,
    state: p.state,
    arrest: p.arrest,
    date: p.date,
    desc: p.desc,
    addr: p.addr,
    lng: p.lng,
    lat: p.lat,
  };
}

function respond(msg: WorkerResponse) {
  postMessage(msg);
}

/** Checks whether `outer` fully contains `inner`. */
function bboxContains(outer: BBox, inner: BBox): boolean {
  return (
    outer[0] <= inner[0] && // west
    outer[1] <= inner[1] && // south
    outer[2] >= inner[2] && // east
    outer[3] >= inner[3] // north
  );
}

/** Expands a bbox by the given factor (1.5 = 25% padding on each side). */
function padBbox(bbox: BBox, factor: number): BBox {
  const [west, south, east, north] = bbox;
  const dLng = ((east - west) * (factor - 1)) / 2;
  const dLat = ((north - south) * (factor - 1)) / 2;
  return [
    Math.max(west - dLng, -180),
    Math.max(south - dLat, -90),
    Math.min(east + dLng, 180),
    Math.min(north + dLat, 90),
  ];
}

/** Whether the current cache covers the given bbox and zoom. */
function cacheCovers(bbox: BBox, zoom: number): boolean {
  if (!cachedBbox || cachedZoom === null || !dataReady) return false;
  if (Math.abs(zoom - cachedZoom) > MAX_ZOOM_DELTA) return false;
  return bboxContains(cachedBbox, bbox);
}

// -- FlatGeobuf spatial loading --

/**
 * Loads features from FlatGeobuf for the given padded bbox using HTTP
 * range requests with the spatial index. Returns false if the load was
 * cancelled by a newer request (detected via sequence number).
 */
async function loadFeaturesForBbox(
  paddedBbox: BBox,
  zoom: number,
  seq: number,
): Promise<boolean> {
  const [west, south, east, north] = paddedBbox;
  const url = `${baseUrl}/incidents.fgb`;

  respond({ type: "progress", loaded: 0, total: null, phase: "loading" });

  const iter = flatgeobuf.deserialize(url, {
    minX: west,
    minY: south,
    maxX: east,
    maxY: north,
  });

  const points: CrimePoint[] = [];
  const features: GeoJSON.Feature<GeoJSON.Point, CrimePoint>[] = [];

  for await (const feature of iter) {
    // Check for stale load
    if (seq !== loadSeq) return false;

    const f = feature as GeoJSON.Feature<GeoJSON.Point>;
    const props = f.properties;
    if (!props) continue;

    const [lng, lat] = f.geometry.coordinates;
    const point: CrimePoint = {
      id: props.id as number,
      lng,
      lat,
      sid: (props.sid as string) ?? "",
      subcategory: (props.subcategory as string) ?? "",
      category: (props.category as string) ?? "",
      severity: (props.severity as number) ?? 1,
      city: (props.city as string) ?? "",
      state: (props.state as string) ?? "",
      arrest: (props.arrest as boolean | null) ?? null,
      date: (props.date as string) ?? "",
      desc: (props.desc as string | null) ?? null,
      addr: (props.addr as string | null) ?? null,
    };

    points.push(point);
    features.push({
      type: "Feature",
      geometry: { type: "Point", coordinates: [lng, lat] },
      properties: point,
    });

    if (points.length % PROGRESS_INTERVAL === 0) {
      respond({
        type: "progress",
        loaded: points.length,
        total: null,
        phase: "loading",
      });
    }
  }

  // Final stale check before committing to cache
  if (seq !== loadSeq) return false;

  // Commit to cache
  cachedPoints = points;
  cachedFeatures = features;
  cachedBbox = paddedBbox;
  cachedZoom = zoom;

  // Build indexes
  respond({
    type: "progress",
    loaded: points.length,
    total: points.length,
    phase: "indexing",
  });

  rebuildClusterIndex();
  dataReady = true;

  respond({ type: "loadComplete", featureCount: points.length });
  return true;
}

// -- Filtering + Supercluster --

/**
 * Rebuilds the Supercluster index and rbush sidebar tree from cached
 * points, applying current filters.
 */
function rebuildClusterIndex() {
  const predicate = currentFilters
    ? buildIncidentPredicate(currentFilters)
    : null;

  const features: GeoJSON.Feature<GeoJSON.Point, CrimePoint>[] = [];
  filteredPoints = [];

  if (predicate) {
    for (let i = 0; i < cachedPoints.length; i++) {
      if (predicate(cachedPoints[i])) {
        filteredPoints.push(cachedPoints[i]);
        features.push(cachedFeatures[i]);
      }
    }
  } else {
    filteredPoints = cachedPoints;
    features.push(...cachedFeatures);
  }

  // Build Supercluster index
  clusterIndex = new Supercluster({
    radius: SUPERCLUSTER_RADIUS,
    maxZoom: SUPERCLUSTER_MAX_ZOOM,
    map: (props) => ({
      severitySum: (props as CrimePoint).severity,
      count: 1,
    }),
    reduce: (accumulated, props) => {
      accumulated.severitySum += props.severitySum;
      accumulated.count += props.count;
    },
  });

  clusterIndex.load(features);

  // Build rbush R-tree for sidebar bbox queries
  const rbushItems: RBushItem[] = new Array(filteredPoints.length);
  for (let i = 0; i < filteredPoints.length; i++) {
    const p = filteredPoints[i];
    rbushItems[i] = {
      minX: p.lng,
      minY: p.lat,
      maxX: p.lng,
      maxY: p.lat,
      idx: i,
    };
  }
  sidebarTree = new RBush<RBushItem>();
  sidebarTree.load(rbushItems);
}

/**
 * Get filtered points within a bounding box using rbush.
 * O(log n + k) instead of O(n).
 */
function getFilteredPointsInBbox(bbox: BBox): CrimePoint[] {
  const [west, south, east, north] = bbox;
  const results = sidebarTree.search({
    minX: west,
    minY: south,
    maxX: east,
    maxY: north,
  });
  return results.map((item) => filteredPoints[item.idx]);
}

// -- Message handlers --

function handleInit(bUrl: string) {
  baseUrl = bUrl;
  // No bulk load — data is loaded on demand per viewport.
  // Send ready immediately so the main thread knows the worker is alive.
  respond({ type: "ready", featureCount: 0 });
}

function handleSetFilters(filters: FilterState) {
  currentFilters = filters;
  if (dataReady) {
    rebuildClusterIndex();
  }
}

/**
 * Handles a viewport request. If the cache covers the bbox, queries
 * the existing indexes instantly. Otherwise, triggers a spatial load
 * from FlatGeobuf with a padded bbox, builds indexes, then responds.
 */
async function handleGetViewport(bbox: BBox, zoom: number, seq: number) {
  latestViewportSeq = seq;
  lastRequestedBbox = bbox;
  lastRequestedZoom = zoom;

  if (cacheCovers(bbox, zoom)) {
    // Cache hit — query existing indexes instantly
    respondWithViewport(bbox, zoom, seq);
    return;
  }

  // Cache miss — load features for the padded bbox
  const paddedBbox = padBbox(bbox, BBOX_PADDING_FACTOR);
  const mySeq = ++loadSeq;

  const loaded = await loadFeaturesForBbox(paddedBbox, zoom, mySeq);
  if (!loaded) return; // Stale, a newer request superseded this one

  // Check we're still the latest viewport request
  if (seq < latestViewportSeq) return;

  respondWithViewport(bbox, zoom, seq);
}

/** Queries existing Supercluster + rbush and sends the viewport response. */
function respondWithViewport(bbox: BBox, zoom: number, seq: number) {
  if (!dataReady || !clusterIndex) {
    respond({
      type: "viewport",
      clusters: { type: "FeatureCollection", features: [] },
      sidebarFeatures: [],
      totalCount: 0,
      seq,
    });
    return;
  }

  // Check for staleness
  if (seq < latestViewportSeq) return;

  const clusterFeatures = clusterIndex.getClusters(bbox, zoom);

  if (seq < latestViewportSeq) return;

  const clusters: GeoJSON.FeatureCollection = {
    type: "FeatureCollection",
    features: clusterFeatures,
  };

  // Sidebar features via rbush
  const viewportPoints = getFilteredPointsInBbox(bbox);
  viewportPoints.sort(
    (a, b) => (a.date > b.date ? -1 : a.date < b.date ? 1 : 0),
  );

  viewportSidebarCache = viewportPoints.map(pointToSidebarFeature);
  const sidebarPage = viewportSidebarCache.slice(0, 50);

  if (seq < latestViewportSeq) return;

  respond({
    type: "viewport",
    clusters,
    sidebarFeatures: sidebarPage,
    totalCount: viewportSidebarCache.length,
    seq,
  });
}

function handleGetExpansionZoom(clusterId: number) {
  if (!clusterIndex) {
    respond({ type: "error", message: "Cluster index not initialized" });
    return;
  }

  const zoom = clusterIndex.getClusterExpansionZoom(clusterId);
  respond({ type: "expansionZoom", clusterId, zoom });
}

function handleGetMoreSidebar(offset: number, limit: number) {
  const features = viewportSidebarCache.slice(offset, offset + limit);
  respond({
    type: "moreSidebarFeatures",
    features,
    hasMore: offset + limit < viewportSidebarCache.length,
    offset,
  });
}

// -- Message dispatch --

self.onmessage = async (e: MessageEvent<WorkerRequest>) => {
  const msg = e.data;

  try {
    switch (msg.type) {
      case "init":
        handleInit(msg.baseUrl);
        break;

      case "setFilters":
        handleSetFilters(msg.filters);
        // Re-query the current viewport with new filters if we have one
        if (dataReady && lastRequestedBbox && lastRequestedZoom !== null) {
          const seq = latestViewportSeq;
          respondWithViewport(lastRequestedBbox, lastRequestedZoom, seq);
        }
        break;

      case "getViewport":
        await handleGetViewport(msg.bbox, msg.zoom, msg.seq);
        break;

      case "getExpansionZoom":
        handleGetExpansionZoom(msg.clusterId);
        break;

      case "getMoreSidebarFeatures":
        handleGetMoreSidebar(msg.offset, msg.limit);
        break;
    }
  } catch (err) {
    respond({
      type: "error",
      message: err instanceof Error ? err.message : String(err),
    });
  }
};
