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
 * - 8+: clusters (server-side DuckDB k-means) + individual points (PMTiles)
 *
 * Clusters and individual dots coexist at zoom 8+. Clusters render on top
 * and are only shown when count >= minClusterCount. Individual dots show
 * through in sparse areas where no cluster covers them.
 */
export const HEATMAP_MAX_ZOOM = 8;

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

/**
 * Minimum incident count for a cluster to be rendered. Clusters below
 * this threshold are not shown — individual PMTiles dots handle sparse
 * areas instead.
 */
export const CLUSTER_MIN_COUNT: number = clusterConfig.minClusterCount;
