/** MapLibre configuration and layer definitions. */

import hexbinConfig from "@config/hexbins.json";

/** Default map center: geographic center of the contiguous US. */
export const DEFAULT_CENTER: [number, number] = [-98.5795, 39.8283];
export const DEFAULT_ZOOM = 4;

/** CARTO vector basemap style URLs (free, no API key). */
export const DARK_STYLE =
  "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json";
export const LIGHT_STYLE =
  "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json";

/**
 * The first label layer ID AFTER roads/bridges/buildings in each CARTO style.
 * Hexbin and heatmap layers are inserted before this layer so they render
 * below map labels but above roads.
 *
 * Positron places `waterway_label` before roads, so we use `watername_ocean`.
 * Dark Matter places `waterway_label` after roads, so it works directly.
 */
export const LABEL_BEFORE_ID: Record<"light" | "dark", string> = {
  dark: "waterway_label",
  light: "watername_ocean",
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

/** Theme-dependent hex outline color. */
export function hexOutlineColor(theme: "light" | "dark"): string {
  return theme === "dark" ? "#ff6b6b" : "#a50f15";
}

/** Theme-dependent point stroke (border) color. */
export function pointStrokeColor(theme: "light" | "dark"): string {
  return theme === "dark" ? "#1a1a2e" : "#ffffff";
}
