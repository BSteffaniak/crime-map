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
 * Server-side cluster grid divisors by zoom level.
 *
 * The `count_summary` table uses 0.001-degree cells (longitude/latitude * 1000).
 * These divisors group cells into coarser grids for cluster aggregation.
 * The server uses the same values; these are documented here for reference.
 *
 * | Zoom | Divisor | Effective grid |
 * |------|---------|----------------|
 * | 8    | 80      | ~0.08 degree   |
 * | 9    | 40      | ~0.04 degree   |
 * | 10   | 20      | ~0.02 degree   |
 * | 11   | 10      | ~0.01 degree   |
 */
export const CLUSTER_GRID_DIVISORS: Record<number, number> = {
  8: 80,
  9: 40,
  10: 20,
  11: 10,
};
