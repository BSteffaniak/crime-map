/** Types for the cluster web worker message protocol. */

import type { FilterState } from "../types";

/** A normalized crime incident point used in the worker. */
export interface CrimePoint {
  id: number;
  lng: number;
  lat: number;
  sid: string;
  subcategory: string;
  category: string;
  severity: number;
  city: string;
  state: string;
  arrest: boolean | null;
  date: string;
  desc: string | null;
  addr: string | null;
}

/** Feature data sent to the sidebar for display. */
export interface SidebarFeature {
  id: number;
  sid: string;
  subcategory: string;
  category: string;
  severity: number;
  city: string;
  state: string;
  arrest: boolean | null;
  date: string;
  desc: string | null;
  addr: string | null;
  lng: number;
  lat: number;
}

/** Bounding box as [west, south, east, north]. */
export type BBox = [number, number, number, number];

// -- Request messages (main thread -> worker) --

export interface InitRequest {
  type: "init";
  baseUrl: string;
}

export interface SetFiltersRequest {
  type: "setFilters";
  filters: FilterState;
}

export interface GetViewportRequest {
  type: "getViewport";
  bbox: BBox;
  zoom: number;
  /** Sequence number for request cancellation. */
  seq: number;
}

export interface GetExpansionZoomRequest {
  type: "getExpansionZoom";
  clusterId: number;
}

export interface GetMoreSidebarRequest {
  type: "getMoreSidebarFeatures";
  offset: number;
  limit: number;
}

export type WorkerRequest =
  | InitRequest
  | SetFiltersRequest
  | GetViewportRequest
  | GetExpansionZoomRequest
  | GetMoreSidebarRequest;

// -- Response messages (worker -> main thread) --

export interface ReadyResponse {
  type: "ready";
  featureCount: number;
}

/** Sent periodically during a viewport FlatGeobuf load. */
export interface ProgressResponse {
  type: "progress";
  loaded: number;
  /** Approximate total if known, null otherwise. */
  total: number | null;
  phase: "loading" | "indexing";
}

/** Sent when a viewport spatial load completes and indexes are built. */
export interface LoadCompleteResponse {
  type: "loadComplete";
  featureCount: number;
}

export interface ViewportResponse {
  type: "viewport";
  clusters: GeoJSON.FeatureCollection;
  sidebarFeatures: SidebarFeature[];
  totalCount: number;
  /** Echoed sequence number for stale response detection. */
  seq: number;
}

export interface ExpansionZoomResponse {
  type: "expansionZoom";
  clusterId: number;
  zoom: number;
}

export interface MoreSidebarResponse {
  type: "moreSidebarFeatures";
  features: SidebarFeature[];
  hasMore: boolean;
  offset: number;
}

export interface ErrorResponse {
  type: "error";
  message: string;
}

export type WorkerResponse =
  | ReadyResponse
  | ProgressResponse
  | LoadCompleteResponse
  | ViewportResponse
  | ExpansionZoomResponse
  | MoreSidebarResponse
  | ErrorResponse;
