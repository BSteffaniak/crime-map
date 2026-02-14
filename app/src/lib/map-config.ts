/** MapLibre configuration and layer definitions. */

import type { StyleSpecification } from "maplibre-gl";
import clusterConfig from "@config/clusters.json";

/** Default map center: geographic center of the contiguous US. */
export const DEFAULT_CENTER: [number, number] = [-98.5795, 39.8283];
export const DEFAULT_ZOOM = 4;

/** Base map style using free OpenStreetMap tiles. */
export const MAP_STYLE: StyleSpecification = {
  version: 8,
  sources: {
    osm: {
      type: "raster",
      tiles: ["https://tile.openstreetmap.org/{z}/{x}/{y}.png"],
      tileSize: 256,
      attribution: "&copy; OpenStreetMap contributors",
    },
  },
  layers: [
    {
      id: "osm-tiles",
      type: "raster",
      source: "osm",
      minzoom: 0,
      maxzoom: 19,
    },
  ],
};

/**
 * Zoom thresholds for switching between visualization modes:
 * - 0-7: heatmap
 * - 8-11: clusters (server-side DuckDB aggregation)
 * - 12+: individual points
 */
export const HEATMAP_MAX_ZOOM = 8;
export const CLUSTER_MAX_ZOOM = 12;

/**
 * Computes the target cluster count for a given zoom level using the
 * shared config from `config/clusters.json`.
 *
 * The density base is multiplied by the zoom-specific multiplier to
 * produce more clusters as the user zooms in.
 */
export function clusterTargetK(zoom: number): number {
  const z = Math.floor(zoom).toString();
  const multiplier =
    (clusterConfig.zoomMultipliers as Record<string, number>)[z] ?? 1.5;
  return Math.round(clusterConfig.density * multiplier);
}
