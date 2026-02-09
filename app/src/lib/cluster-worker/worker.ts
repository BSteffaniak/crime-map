/**
 * Cluster web worker for crime map.
 *
 * Loads crime incidents from FlatGeobuf via HTTP range requests,
 * filters them using JS predicates, and feeds them into Supercluster
 * for mid-zoom clustering. Returns cluster GeoJSON and sidebar feature
 * lists to the main thread.
 */

import Supercluster from "supercluster";
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

// -- State --

let baseUrl = "";
const allPoints: CrimePoint[] = [];
const loadedIds = new Set<number>();
let filteredPoints: CrimePoint[] = [];
let currentFilters: FilterState | null = null;
let clusterIndex: Supercluster | null = null;

/** Sidebar features for the current viewport, cached for pagination. */
let viewportSidebarCache: SidebarFeature[] = [];

// Supercluster config: clusters visible at zoom 8-11 (CLUSTER_MAX_ZOOM = 12)
const SUPERCLUSTER_MAX_ZOOM = 11;
const SUPERCLUSTER_RADIUS = 60;

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

function pointToGeoJSON(
  p: CrimePoint,
): GeoJSON.Feature<GeoJSON.Point, CrimePoint> {
  return {
    type: "Feature",
    geometry: { type: "Point", coordinates: [p.lng, p.lat] },
    properties: p,
  };
}

function respond(msg: WorkerResponse) {
  postMessage(msg);
}

// -- FlatGeobuf loading --

async function loadFeaturesInBbox(url: string, bbox: BBox): Promise<number> {
  const fgbBbox = {
    minX: bbox[0],
    minY: bbox[1],
    maxX: bbox[2],
    maxY: bbox[3],
  };

  let added = 0;

  const iter = flatgeobuf.deserialize(url, fgbBbox);
  for await (const feature of iter) {
    const f = feature as GeoJSON.Feature<GeoJSON.Point>;
    const props = f.properties;
    if (!props) continue;

    const id = props.id as number;
    if (loadedIds.has(id)) continue;
    loadedIds.add(id);

    const [lng, lat] = f.geometry.coordinates;
    allPoints.push({
      id,
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
    added++;
  }

  return added;
}

// -- Filtering + Supercluster --

function rebuildClusterIndex() {
  const predicate = currentFilters
    ? buildIncidentPredicate(currentFilters)
    : () => true;

  filteredPoints = allPoints.filter(predicate);

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

  const features = filteredPoints.map(pointToGeoJSON);
  clusterIndex.load(features);
}

/** Get filtered points within a bounding box for the sidebar. */
function getFilteredPointsInBbox(bbox: BBox): CrimePoint[] {
  const [west, south, east, north] = bbox;
  return filteredPoints.filter(
    (p) => p.lng >= west && p.lng <= east && p.lat >= south && p.lat <= north,
  );
}

// -- Message handlers --

function handleInit(bUrl: string) {
  baseUrl = bUrl;
}

function handleSetFilters(filters: FilterState) {
  currentFilters = filters;
  rebuildClusterIndex();
}

async function handleGetViewport(bbox: BBox, zoom: number) {
  // Load any new features from FlatGeobuf for this bbox
  const url = `${baseUrl}/incidents.fgb`;
  const added = await loadFeaturesInBbox(url, bbox);

  // If new features were loaded, rebuild cluster index
  if (added > 0) {
    rebuildClusterIndex();
  }

  // Get clusters from Supercluster
  let clusters: GeoJSON.FeatureCollection = {
    type: "FeatureCollection",
    features: [],
  };

  if (clusterIndex) {
    const clusterFeatures = clusterIndex.getClusters(bbox, zoom);
    clusters = {
      type: "FeatureCollection",
      features: clusterFeatures,
    };
  }

  // Get sidebar features (filtered points in viewport, sorted by date desc)
  const viewportPoints = getFilteredPointsInBbox(bbox);
  viewportPoints.sort((a, b) => (a.date > b.date ? -1 : a.date < b.date ? 1 : 0));

  viewportSidebarCache = viewportPoints.map(pointToSidebarFeature);

  const sidebarPage = viewportSidebarCache.slice(0, 50);

  respond({
    type: "viewport",
    clusters,
    sidebarFeatures: sidebarPage,
    totalCount: viewportSidebarCache.length,
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
        respond({ type: "ready", featureCount: allPoints.length });
        break;

      case "setFilters":
        handleSetFilters(msg.filters);
        break;

      case "getViewport":
        await handleGetViewport(msg.bbox, msg.zoom);
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
