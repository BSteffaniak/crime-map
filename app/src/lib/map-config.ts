/** MapLibre configuration and layer definitions. */

import { layers, namedFlavor } from "@protomaps/basemaps";
import type { StyleSpecification } from "maplibre-gl";
import hexbinConfig from "@config/hexbins.json";

/**
 * PMTiles URL for crime incident data. Configurable via `VITE_TILES_URL`
 * environment variable at build time.
 *
 * - Local dev: defaults to `pmtiles:///tiles/incidents.pmtiles` (served by
 *   the Rust backend via Vite proxy)
 * - Production: set to a CDN URL, e.g.
 *   `pmtiles://https://pub-xxx.r2.dev/incidents.pmtiles`
 */
export const INCIDENTS_PMTILES_URL: string =
  import.meta.env.VITE_TILES_URL ?? "pmtiles:///tiles/incidents.pmtiles";

/** Default map center: geographic center of the contiguous US. */
export const DEFAULT_CENTER: [number, number] = [-98.5795, 39.8283];
export const DEFAULT_ZOOM = 4;

/**
 * All available map themes. Each maps to a Protomaps flavor and a base
 * UI theme ("light" or "dark") that controls Tailwind classes and
 * overlay styling.
 */
export const MAP_THEMES = ["light", "dark", "white", "grayscale", "black"] as const;
export type MapTheme = (typeof MAP_THEMES)[number];

/** Maps each map theme to the base UI theme used for Tailwind/overlay colors. */
export function baseUiTheme(mapTheme: MapTheme): "light" | "dark" {
  switch (mapTheme) {
    case "light":
    case "white":
    case "grayscale":
      return "light";
    case "dark":
    case "black":
      return "dark";
  }
}

/** Default map theme for each UI base theme. */
export function defaultMapTheme(uiTheme: "light" | "dark"): MapTheme {
  return uiTheme;
}

// ---------------------------------------------------------------------------
// Protomaps style generation
// ---------------------------------------------------------------------------

/**
 * Protomaps basemap tile source URL.
 *
 * - **Production**: set `VITE_PROTOMAPS_API_KEY` at build time to use the
 *   hosted API (`api.protomaps.com`) which serves MVT tiles via TileJSON
 *   with proper CORS headers. You must add your production origin(s) to
 *   the key's allowed-origins list at https://protomaps.com/account.
 * - **Local dev**: when no API key is set, falls back to the daily PMTiles
 *   build from `build.protomaps.com` (works from localhost without CORS
 *   issues).
 */
function protomapsTileSource(): string {
  const apiKey = import.meta.env.VITE_PROTOMAPS_API_KEY as string | undefined;
  if (apiKey) {
    return `https://api.protomaps.com/tiles/v4.json?key=${apiKey}`;
  }
  // Fallback: daily build PMTiles (localhost only — CORS blocks other origins)
  const d = new Date();
  d.setDate(d.getDate() - 1);
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `pmtiles://https://build.protomaps.com/${yyyy}${mm}${dd}.pmtiles`;
}

const BASEMAP_SOURCE = protomapsTileSource();
const GLYPHS_URL =
  "https://protomaps.github.io/basemaps-assets/fonts/{fontstack}/{range}.pbf";
const SPRITE_BASE =
  "https://protomaps.github.io/basemaps-assets/sprites/v4";

const SOURCE_NAME = "protomaps";

/**
 * Builds a full MapLibre `StyleSpecification` for a Protomaps theme with
 * the "data sandwich" pattern: base layers, then a gap for data layers
 * (inserted at runtime via `beforeId`), then label layers on top.
 */
export function buildProtomapsStyle(theme: MapTheme): StyleSpecification {
  const flavor = namedFlavor(theme);

  // Base geometry layers (no labels)
  const baseLayers = layers(SOURCE_NAME, flavor);

  // Label layers only
  const labelLayers = layers(SOURCE_NAME, flavor, {
    lang: "en",
    labelsOnly: true,
  });

  // Boost text halo for readability over data layers (hexbins, heatmap).
  // Protomaps defaults to halo-width 1 with no blur — too thin to read
  // over the red hexbin fill. Widening to 2 with a soft 0.5 blur creates
  // a clear knockout around each character.
  for (const layer of labelLayers) {
    if (layer.type === "symbol" && layer.paint) {
      const paint = layer.paint as Record<string, unknown>;
      if ("text-halo-width" in paint) {
        paint["text-halo-width"] = 2;
        paint["text-halo-blur"] = 0.5;
      }
    }
  }

  return {
    version: 8,
    glyphs: GLYPHS_URL,
    sprite: `${SPRITE_BASE}/${theme}`,
    sources: {
      [SOURCE_NAME]: {
        type: "vector",
        url: BASEMAP_SOURCE,
        attribution:
          '<a href="https://openstreetmap.org/copyright">OpenStreetMap</a> | <a href="https://protomaps.com">Protomaps</a>',
      },
    },
    layers: [...baseLayers, ...labelLayers],
  } as StyleSpecification;
}

/**
 * The first label layer ID in Protomaps styles. Data layers (hexbins,
 * heatmap) are inserted before this layer so they render below map
 * labels but above roads — the "data sandwich" pattern.
 */
export const LABEL_BEFORE_ID = "address_label";

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
 * Debounce delay (ms) for viewport-driven data fetches (hexbins, sidebar).
 * Applied when the map is panning within the same hex resolution bracket.
 * Skipped on `moveend` (settled) and on hex resolution changes.
 */
export const VIEWPORT_DEBOUNCE_MS = 150;

/**
 * Throttle interval (ms) for emitting viewport updates during continuous
 * map movement (`move` events). Controls how often mid-pan data fetches
 * can be triggered. Lower = more frequent updates, higher network load.
 */
export const VIEWPORT_THROTTLE_MS = 300;

/**
 * Returns the H3 resolution for a given zoom level using the shared
 * config from `config/hexbins.json`.
 */
export function hexbinResolution(zoom: number): number {
  const z = Math.floor(zoom).toString();
  return (hexbinConfig.zoomResolutionMap as Record<string, number>)[z] ?? 5;
}

/** Minimum count for a hexbin to be rendered. */
export const HEX_MIN_COUNT: number = hexbinConfig.minCount;

/**
 * Single hex fill color per base UI theme. Density is represented via
 * opacity, not color variation.
 */
export function hexFillColor(theme: "light" | "dark"): string {
  return (hexbinConfig.hexFillColor as Record<string, string>)[theme];
}

/**
 * Base opacity range [min, max] for hex fill before zoom scaling.
 * Low-count hexes get min opacity; high-count hexes get max.
 */
export const HEX_OPACITY_RANGE: [number, number] =
  hexbinConfig.hexFillOpacityRange as [number, number];

/**
 * Returns the hex fill opacity envelope for a given zoom level,
 * interpolating between configured breakpoints. This value scales the
 * per-feature opacity range.
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

/** Max hex outline stroke opacity (at highest count). */
export const HEX_STROKE_OPACITY: number = hexbinConfig.hexStrokeOpacity;

/** Theme-dependent hex outline color. */
export function hexOutlineColor(theme: "light" | "dark"): string {
  return (hexbinConfig.hexOutlineColor as Record<string, string>)[theme];
}

/** Theme-dependent point stroke (border) color. */
export function pointStrokeColor(theme: "light" | "dark"): string {
  return theme === "dark" ? "#1a1a2e" : "#ffffff";
}

// ---------------------------------------------------------------------------
// Boundaries PMTiles
// ---------------------------------------------------------------------------

/**
 * PMTiles URL for boundary data. Configurable via `VITE_BOUNDARIES_TILES_URL`
 * environment variable at build time.
 */
export const BOUNDARIES_PMTILES_URL: string =
  import.meta.env.VITE_BOUNDARIES_TILES_URL ?? "pmtiles:///tiles/boundaries.pmtiles";

// ---------------------------------------------------------------------------
// Map layer configuration
// ---------------------------------------------------------------------------

/** A configurable map layer that users can toggle on/off. */
export interface MapLayerConfig {
  /** Unique identifier (e.g. "heatmap", "states"). */
  id: string;
  /** Display label (e.g. "Heatmap", "State Boundaries"). */
  label: string;
  /** Grouping for the UI. */
  group: "crime" | "boundaries";
  /** Whether this layer is visible by default. */
  defaultVisible: boolean;
  /** Minimum zoom level at which this layer is useful. */
  minZoom?: number;
}

/** Registry of all toggleable map layers. */
export const MAP_LAYERS: MapLayerConfig[] = [
  // Crime data layers
  { id: "heatmap", label: "Heatmap", group: "crime", defaultVisible: true },
  { id: "hexbins", label: "Hex Density", group: "crime", defaultVisible: true },
  { id: "points", label: "Incident Points", group: "crime", defaultVisible: true, minZoom: POINTS_MIN_ZOOM },
  // Boundary layers
  { id: "states", label: "State Boundaries", group: "boundaries", defaultVisible: false },
  { id: "counties", label: "County Boundaries", group: "boundaries", defaultVisible: false, minZoom: 4 },
  { id: "places", label: "City/Town Boundaries", group: "boundaries", defaultVisible: false, minZoom: 7 },
  { id: "tracts", label: "Census Tracts", group: "boundaries", defaultVisible: false, minZoom: 9 },
  { id: "neighborhoods", label: "Neighborhoods", group: "boundaries", defaultVisible: false, minZoom: 9 },
];

/** Returns the default layer visibility map. */
export function defaultLayerVisibility(): Record<string, boolean> {
  const result: Record<string, boolean> = {};
  for (const layer of MAP_LAYERS) {
    result[layer.id] = layer.defaultVisible;
  }
  return result;
}
