/** MapLibre configuration and layer definitions. */

import type { StyleSpecification } from "maplibre-gl";
import hexbinConfig from "@config/hexbins.json";

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
 * - 0-7: heatmap only
 * - 8-13: H3 hexagonal choropleth (heatmap fades underneath)
 * - 14+: individual incident dots (PMTiles) appear alongside hexbins
 *
 * The heatmap persists underneath hexbins with decreasing opacity.
 */
export const HEATMAP_MAX_ZOOM = 8;

/** Minimum zoom level at which individual incident dots appear. */
export const POINTS_MIN_ZOOM = 13;

/**
 * Returns the H3 resolution for a given zoom level using the shared
 * config from `config/hexbins.json`.
 */
export function hexbinResolution(zoom: number): number {
  const z = Math.floor(zoom).toString();
  return (hexbinConfig.zoomResolutionMap as Record<string, number>)[z] ?? 5;
}

/** Sequential red color scale for hex fill (5 steps, light to dark). */
export const HEX_COLOR_SCALE: string[] = hexbinConfig.colorScale;

/** Minimum count for a hexbin to be rendered. */
export const HEX_MIN_COUNT: number = hexbinConfig.minCount;

/**
 * Returns the hex fill opacity for a given zoom level, interpolating
 * between configured breakpoints.
 */
export function hexFillOpacity(zoom: number): number {
  const entries = Object.entries(hexbinConfig.hexFillOpacity)
    .map(([z, o]) => [Number(z), o as number] as const)
    .sort((a, b) => a[0] - b[0]);

  if (entries.length === 0) return 0.5;
  if (zoom <= entries[0][0]) return entries[0][1];
  if (zoom >= entries[entries.length - 1][0]) return entries[entries.length - 1][1];

  // Linear interpolation between surrounding breakpoints
  for (let i = 0; i < entries.length - 1; i++) {
    if (zoom >= entries[i][0] && zoom <= entries[i + 1][0]) {
      const t = (zoom - entries[i][0]) / (entries[i + 1][0] - entries[i][0]);
      return entries[i][1] + t * (entries[i + 1][1] - entries[i][1]);
    }
  }
  return 0.5;
}

/** Hex outline stroke opacity. */
export const HEX_STROKE_OPACITY: number = hexbinConfig.hexStrokeOpacity;
