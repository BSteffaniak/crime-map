/**
 * React hook for fetching server-side H3 hexbin data.
 *
 * Queries GET /api/hexbins with the current viewport, zoom, and filters.
 * The response is MessagePack-encoded and decoded client-side. Hexbins
 * are fetched at every zoom level; the server picks the appropriate H3
 * resolution based on the zoom parameter via config/hexbins.json.
 *
 * Fetch scheduling:
 * - On `moveend` (settled=true): fetches immediately, no debounce.
 * - On `move` with H3 resolution change: fetches immediately (zoom
 *   bracket crossing should feel instant).
 * - On `move` within the same resolution: debounced by VIEWPORT_DEBOUNCE_MS
 *   to batch rapid mid-pan updates.
 */

import { type MutableRefObject, useEffect, useRef, useState } from "react";
import { decode } from "@msgpack/msgpack";
import type { FilterState, CategoryId } from "../types";
import { CRIME_CATEGORIES } from "../types";
import type { BBox } from "../sidebar/types";
import type { HexbinEntry } from "./types";
import { VIEWPORT_DEBOUNCE_MS, hexbinResolution } from "../map-config";

/**
 * Expands a bounding box by a zoom-dependent buffer so hexbins beyond the
 * visible viewport edge are pre-fetched. This prevents "pop in" at the
 * edges when panning, especially at higher zoom levels where individual
 * hexbins are large and visually prominent.
 *
 * | Zoom   | Factor (per side) | Total coverage |
 * |--------|-------------------|----------------|
 * | 0-7    | 0.10              | ~1.2x × 1.2x  |
 * | 8-11   | 0.20              | ~1.4x × 1.4x  |
 * | 12+    | 0.25              | ~1.5x × 1.5x  |
 */
function expandBbox(bbox: BBox, zoom: number): BBox {
  const factor = zoom >= 12 ? 0.25 : zoom >= 8 ? 0.2 : 0.1;
  const [west, south, east, north] = bbox;
  const dLng = (east - west) * factor;
  const dLat = (north - south) * factor;
  return [
    Math.max(west - dLng, -180),
    Math.max(south - dLat, -90),
    Math.min(east + dLng, 180),
    Math.min(north + dLat, 90),
  ];
}

/**
 * Builds the query string for the hexbins API.
 */
function buildQueryString(
  bbox: BBox,
  zoom: number,
  filters: FilterState,
): string {
  const params = new URLSearchParams();

  params.set("bbox", bbox.join(","));
  params.set("zoom", String(Math.floor(zoom)));

  // Date filters
  if (filters.dateFrom) {
    params.set("from", filters.dateFrom);
  }
  if (filters.dateTo) {
    params.set("to", filters.dateTo);
  }

  // Category / subcategory filters
  if (filters.subcategories.length > 0) {
    params.set("subcategories", filters.subcategories.join(","));
  } else if (filters.categories.length > 0) {
    const expanded: string[] = [];
    for (const catId of filters.categories) {
      const cat = CRIME_CATEGORIES[catId as CategoryId];
      if (cat) {
        for (const sub of cat.subcategories) {
          expanded.push(sub.id);
        }
      }
    }
    if (expanded.length > 0) {
      params.set("subcategories", expanded.join(","));
    }
  }

  // Severity filter
  if (filters.severityMin > 1) {
    params.set("severityMin", String(filters.severityMin));
  }

  // Arrest filter
  if (filters.arrestMade !== null) {
    params.set("arrestMade", String(filters.arrestMade));
  }

  return params.toString();
}

interface UseHexbinsResult {
  hexbins: HexbinEntry[];
  loading: boolean;
}

/**
 * Fetches server-side H3 hexbin data for the current viewport.
 *
 * Active at any zoom >= HEATMAP_MAX_ZOOM. Returns an empty array below
 * that threshold (heatmap handles those zoom levels alone).
 *
 * @param settledRef - ref indicating whether the most recent viewport
 *   update was a `moveend` (true) or a mid-pan `move` (false). When
 *   settled, the fetch fires immediately with no debounce.
 */
export function useHexbins(
  bbox: BBox | null,
  zoom: number,
  filters: FilterState,
  settledRef: MutableRefObject<boolean>,
): UseHexbinsResult {
  const [hexbins, setHexbins] = useState<HexbinEntry[]>([]);
  const [loading, setLoading] = useState(false);

  const abortRef = useRef<AbortController | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const genRef = useRef(0);
  const prevResolutionRef = useRef<number | null>(null);

  useEffect(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
      debounceRef.current = null;
    }

    const currentResolution = hexbinResolution(zoom);
    const resolutionChanged =
      prevResolutionRef.current !== null &&
      currentResolution !== prevResolutionRef.current;
    const settled = settledRef.current;

    // Determine whether to skip debounce:
    // - settled (moveend): always fetch immediately
    // - resolution changed: fetch immediately for instant zoom transitions
    const immediate = settled || resolutionChanged;

    const doFetch = () => {
      abortRef.current?.abort();

      const gen = ++genRef.current;

      if (!bbox) {
        setHexbins([]);
        setLoading(false);
        return;
      }

      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);

      const qs = buildQueryString(expandBbox(bbox, zoom), zoom, filters);

      fetch(`/api/hexbins?${qs}`, { signal: controller.signal })
        .then((res) => {
          if (!res.ok) throw new Error(`Hexbins API ${res.status}`);
          return res.arrayBuffer();
        })
        .then((buffer) => {
          if (gen !== genRef.current) return;
          // rmp_serde::to_vec encodes structs as positional arrays:
          // each entry is [vertices, count] matching Rust field order.
          const raw = decode(new Uint8Array(buffer)) as [[number, number][], number][];
          const data: HexbinEntry[] = raw.map(([vertices, count]) => ({ vertices, count }));
          setHexbins(data);
          setLoading(false);
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          console.error("Hexbins fetch failed:", err);
          if (gen !== genRef.current) return;
          setHexbins([]);
          setLoading(false);
        });
    };

    prevResolutionRef.current = currentResolution;

    if (immediate) {
      doFetch();
    } else {
      debounceRef.current = setTimeout(doFetch, VIEWPORT_DEBOUNCE_MS);
    }

    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
        debounceRef.current = null;
      }
    };
  }, [bbox, zoom, filters, settledRef]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  return { hexbins, loading };
}
