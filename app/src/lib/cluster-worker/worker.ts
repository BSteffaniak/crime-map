/**
 * Cluster web worker for crime map.
 *
 * Loads the entire crime incident dataset from FlatGeobuf at startup,
 * maintains a Supercluster index for mid-zoom clustering, and an rbush
 * R-tree for fast sidebar bbox queries. Panning/zooming after the
 * initial load is purely in-memory with no network requests.
 *
 * Performance characteristics:
 * - Initial load: O(n) streaming from FlatGeobuf (one-time cost)
 * - Pan/zoom viewport query: O(k) where k = clusters in view (~instant)
 * - Filter change: O(n log n) Supercluster rebuild (infrequent user action)
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

// -- State --

/**
 * All loaded crime points. Populated once during bulk load, then immutable.
 * Points are stored as flat objects to minimize per-point overhead.
 */
const allPoints: CrimePoint[] = [];

/** Whether the bulk load has completed. */
let dataReady = false;

/** Filtered subset of allPoints matching current filters. */
let filteredPoints: CrimePoint[] = [];

/** Current filter state from the main thread. */
let currentFilters: FilterState | null = null;

/** Supercluster index for mid-zoom clustering (zoom 8-11). */
let clusterIndex: Supercluster | null = null;

/**
 * Pre-built GeoJSON features for all points. Built once during bulk load.
 * On filter changes, we index into this array to avoid re-creating
 * Feature wrapper objects for every point.
 */
let allFeatures: GeoJSON.Feature<GeoJSON.Point, CrimePoint>[] = [];

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

// Supercluster config: clusters visible at zoom 8-11 (CLUSTER_MAX_ZOOM = 12)
const SUPERCLUSTER_MAX_ZOOM = 11;
const SUPERCLUSTER_RADIUS = 60;

/** How often to send progress updates during bulk load. */
const PROGRESS_INTERVAL = 10_000;

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

// -- Bulk FlatGeobuf loading (Phase 1) --

/**
 * Loads the entire FlatGeobuf dataset in one pass. Sends periodic progress
 * updates to the main thread. After loading, pre-builds the GeoJSON features
 * array and the initial Supercluster index.
 */
async function loadAllFeatures(url: string): Promise<void> {
  respond({ type: "progress", loaded: 0, total: null, phase: "loading" });

  // Fetch the file as a stream and deserialize sequentially. Using a
  // ReadableStream instead of a URL avoids the FlatGeobuf HTTP range-request
  // path which traverses the packed R-tree spatial index — that approach
  // issues thousands of small range requests and does not scale for bulk
  // loading large datasets. A single sequential download is dramatically
  // faster when we need all features anyway.
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to fetch ${url}: ${response.status}`);
  }
  if (!response.body) {
    throw new Error(`Response body is null for ${url}`);
  }
  const iter = flatgeobuf.deserialize(response.body as ReadableStream);

  for await (const feature of iter) {
    const f = feature as GeoJSON.Feature<GeoJSON.Point>;
    const props = f.properties;
    if (!props) continue;

    const [lng, lat] = f.geometry.coordinates;
    allPoints.push({
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
    });

    // Send progress every PROGRESS_INTERVAL points
    if (allPoints.length % PROGRESS_INTERVAL === 0) {
      respond({
        type: "progress",
        loaded: allPoints.length,
        total: null,
        phase: "loading",
      });
    }
  }

  // Phase 5: Pre-build the GeoJSON features array once
  respond({
    type: "progress",
    loaded: allPoints.length,
    total: allPoints.length,
    phase: "indexing",
  });

  allFeatures = new Array(allPoints.length);
  for (let i = 0; i < allPoints.length; i++) {
    const p = allPoints[i];
    allFeatures[i] = {
      type: "Feature",
      geometry: { type: "Point", coordinates: [p.lng, p.lat] },
      properties: p,
    };
  }

  // Build initial cluster index with all points (no filters yet)
  rebuildClusterIndex();

  dataReady = true;

  respond({ type: "loadComplete", featureCount: allPoints.length });
}

// -- Filtering + Supercluster (Phase 2 + Phase 3 + Phase 5) --

/**
 * Rebuilds the Supercluster index and rbush sidebar tree from scratch.
 * Called only when filters change (not on viewport changes).
 *
 * Phase 2: This is now the ONLY place that rebuilds the index.
 * Phase 3: Also rebuilds the rbush tree for sidebar queries.
 * Phase 5: Reuses pre-built GeoJSON features from allFeatures[] to avoid
 *          re-creating Feature wrapper objects.
 */
function rebuildClusterIndex() {
  const predicate = currentFilters
    ? buildIncidentPredicate(currentFilters)
    : null;

  // Filter points and collect their pre-built GeoJSON features
  const features: GeoJSON.Feature<GeoJSON.Point, CrimePoint>[] = [];
  filteredPoints = [];

  if (predicate) {
    for (let i = 0; i < allPoints.length; i++) {
      if (predicate(allPoints[i])) {
        filteredPoints.push(allPoints[i]);
        features.push(allFeatures[i]);
      }
    }
  } else {
    // No filters — use everything directly
    filteredPoints = allPoints;
    features.push(...allFeatures);
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

  // Phase 3: Build rbush R-tree for sidebar bbox queries
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
 * Phase 3: Get filtered points within a bounding box using rbush.
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

async function handleInit(bUrl: string) {
  // Start bulk loading the entire dataset immediately
  const url = `${bUrl}/incidents.fgb`;
  await loadAllFeatures(url);
}

function handleSetFilters(filters: FilterState) {
  currentFilters = filters;
  if (dataReady) {
    rebuildClusterIndex();
  }
}

/**
 * Phase 2: Viewport handler no longer loads data or rebuilds the index.
 * It only queries the existing Supercluster and rbush indexes.
 * Phase 4: Checks sequence number for request cancellation.
 */
function handleGetViewport(bbox: BBox, zoom: number, seq: number) {
  // Phase 4: Record this as the latest request
  latestViewportSeq = seq;

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

  // Phase 4: Check if a newer request has arrived before doing work
  if (seq < latestViewportSeq) return;

  // Get clusters — this is O(k) where k = clusters in view, essentially instant
  const clusterFeatures = clusterIndex.getClusters(bbox, zoom);

  // Phase 4: Check again after cluster query (it's fast, but be safe)
  if (seq < latestViewportSeq) return;

  const clusters: GeoJSON.FeatureCollection = {
    type: "FeatureCollection",
    features: clusterFeatures,
  };

  // Phase 3: Get sidebar features using rbush (O(log n + k))
  const viewportPoints = getFilteredPointsInBbox(bbox);
  viewportPoints.sort(
    (a, b) => (a.date > b.date ? -1 : a.date < b.date ? 1 : 0),
  );

  viewportSidebarCache = viewportPoints.map(pointToSidebarFeature);

  const sidebarPage = viewportSidebarCache.slice(0, 50);

  // Phase 4: Final staleness check before sending response
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
        await handleInit(msg.baseUrl);
        respond({ type: "ready", featureCount: allPoints.length });
        break;

      case "setFilters":
        handleSetFilters(msg.filters);
        break;

      case "getViewport":
        handleGetViewport(msg.bbox, msg.zoom, msg.seq);
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
