/**
 * Sidebar web worker for crime map.
 *
 * Loads crime incident data on demand from a FlatGeobuf file using HTTP
 * range requests with spatial filtering. Only features within a padded
 * viewport bbox are fetched, keeping memory proportional to the visible
 * area.
 *
 * A 1.5x padding factor is applied to the viewport bbox so that small
 * pans are served from the in-memory cache without network requests.
 *
 * Only the "driver" hook (CrimeMap) sends messages to this worker. The
 * "reader" hook (IncidentSidebar) only consumes responses. This avoids
 * sequence number races between multiple hook instances.
 */

import * as flatgeobuf from "flatgeobuf/lib/mjs/geojson.js";
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

const BBOX_PADDING_FACTOR = 1.5;
const MAX_ZOOM_DELTA = 1;
const PROGRESS_INTERVAL = 10_000;

/**
 * Maximum number of features to load from FlatGeobuf per viewport.
 * Prevents OOM at low zoom levels where the bbox covers a large area.
 */
const MAX_FEATURES = 200_000;

// -- State --

let baseUrl = "";
let cachedPoints: CrimePoint[] = [];
let cachedBbox: BBox | null = null;
let cachedZoom: number | null = null;
let dataReady = false;
let filteredPoints: CrimePoint[] = [];
let currentFilters: FilterState | null = null;
let viewportSidebarCache: SidebarFeature[] = [];
let latestSidebarSeq = -1;
let loadSeq = 0;

// -- Helpers --

function toSidebar(p: CrimePoint): SidebarFeature {
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

function bboxContains(outer: BBox, inner: BBox): boolean {
  return (
    outer[0] <= inner[0] &&
    outer[1] <= inner[1] &&
    outer[2] >= inner[2] &&
    outer[3] >= inner[3]
  );
}

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

function cacheCovers(bbox: BBox, zoom: number): boolean {
  if (!cachedBbox || cachedZoom === null || !dataReady) return false;
  if (Math.abs(zoom - cachedZoom) > MAX_ZOOM_DELTA) return false;
  return bboxContains(cachedBbox, bbox);
}

function pointInBbox(p: CrimePoint, bbox: BBox): boolean {
  return p.lng >= bbox[0] && p.lat >= bbox[1] && p.lng <= bbox[2] && p.lat <= bbox[3];
}

// -- FlatGeobuf spatial loading --

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

  for await (const feature of iter) {
    if (seq !== loadSeq) return false;

    const f = feature as GeoJSON.Feature<GeoJSON.Point>;
    const props = f.properties;
    if (!props) continue;

    const [lng, lat] = f.geometry.coordinates;
    points.push({
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

    if (points.length % PROGRESS_INTERVAL === 0) {
      respond({ type: "progress", loaded: points.length, total: null, phase: "loading" });
    }

    // Cap features to prevent OOM at low zoom
    if (points.length >= MAX_FEATURES) {
      break;
    }
  }

  if (seq !== loadSeq) return false;

  cachedPoints = points;
  cachedBbox = paddedBbox;
  cachedZoom = zoom;
  dataReady = true;
  applyFilters();

  respond({ type: "loadComplete", featureCount: points.length });
  return true;
}

// -- Filtering --

function applyFilters() {
  const predicate = currentFilters
    ? buildIncidentPredicate(currentFilters)
    : null;

  if (predicate) {
    filteredPoints = cachedPoints.filter(predicate);
  } else {
    filteredPoints = cachedPoints;
  }
}

// -- Message handlers --

function handleInit(bUrl: string) {
  baseUrl = bUrl;
  respond({ type: "ready" });
}

function handleSetFilters(filters: FilterState) {
  currentFilters = filters;
  if (dataReady) {
    applyFilters();
  }
  // Do NOT auto-respond with sidebar data here. The driver hook
  // will explicitly send a getSidebar request after setFilters.
}

async function handleGetSidebar(bbox: BBox, zoom: number, seq: number) {
  latestSidebarSeq = seq;

  if (cacheCovers(bbox, zoom)) {
    respondWithSidebar(bbox, seq);
    return;
  }

  const paddedBbox = padBbox(bbox, BBOX_PADDING_FACTOR);
  const mySeq = ++loadSeq;

  const loaded = await loadFeaturesForBbox(paddedBbox, zoom, mySeq);
  if (!loaded) {
    // Load was pre-empted by a newer request. Do NOT silently drop —
    // the newer handleGetSidebar call will respond instead.
    return;
  }

  // Check we're still the latest sidebar request
  if (seq < latestSidebarSeq) return;

  respondWithSidebar(bbox, seq);
}

function respondWithSidebar(bbox: BBox, seq: number) {
  if (!dataReady) {
    respond({ type: "sidebar", sidebarFeatures: [], totalCount: 0, seq });
    return;
  }

  if (seq < latestSidebarSeq) return;

  const viewportPoints = filteredPoints.filter((p) => pointInBbox(p, bbox));
  viewportPoints.sort(
    (a, b) => (a.date > b.date ? -1 : a.date < b.date ? 1 : 0),
  );

  viewportSidebarCache = viewportPoints.map(toSidebar);
  const sidebarPage = viewportSidebarCache.slice(0, 50);

  if (seq < latestSidebarSeq) return;

  respond({
    type: "sidebar",
    sidebarFeatures: sidebarPage,
    totalCount: viewportSidebarCache.length,
    seq,
  });
}

function handleGetMoreSidebar(offset: number, limit: number, seq: number) {
  // Seq guard: only serve pagination for the latest sidebar response
  if (seq !== latestSidebarSeq) return;

  const features = viewportSidebarCache.slice(offset, offset + limit);
  respond({
    type: "moreSidebarFeatures",
    features,
    hasMore: offset + limit < viewportSidebarCache.length,
    offset,
    seq,
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
        break;

      case "getSidebar":
        await handleGetSidebar(msg.bbox, msg.zoom, msg.seq);
        break;

      case "getMoreSidebarFeatures":
        handleGetMoreSidebar(msg.offset, msg.limit, msg.seq);
        break;
    }
  } catch (err) {
    respond({
      type: "error",
      message: err instanceof Error ? err.message : String(err),
    });
  }
};
