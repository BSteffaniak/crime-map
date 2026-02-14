/** MapLibre configuration and layer definitions. */

import type { StyleSpecification } from "maplibre-gl";

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
 * Server-side cluster target counts by zoom level.
 *
 * The server uses weighted k-means clustering on fine-grained micro-cells
 * from the `count_summary` table (0.001-degree resolution). The target
 * count controls how many output clusters are produced per viewport.
 *
 * | Zoom | Target clusters |
 * |------|-----------------|
 * | 8    | 40              |
 * | 9    | 60              |
 * | 10   | 80              |
 * | 11   | 100             |
 */
export const CLUSTER_TARGET_COUNTS: Record<number, number> = {
  8: 40,
  9: 60,
  10: 80,
  11: 100,
};
