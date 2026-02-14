/**
 * React hook for fetching server-side H3 hexbin data.
 *
 * Queries GET /api/hexbins with the current viewport, zoom, and filters.
 * The response is MessagePack-encoded and decoded client-side. Active at
 * any zoom level >= HEATMAP_MAX_ZOOM (8). Hexbins render as a choropleth
 * overlay on top of the heatmap, with individual PMTiles dots showing
 * through at high zoom in sparse areas.
 */

import { useEffect, useRef, useState } from "react";
import { decode } from "@msgpack/msgpack";
import type { FilterState, CategoryId } from "../types";
import { CRIME_CATEGORIES } from "../types";
import type { BBox } from "../sidebar/types";
import type { HexbinEntry } from "./types";
import { HEATMAP_MAX_ZOOM } from "../map-config";

/** Debounce delay for hexbin requests (ms). */
const DEBOUNCE_MS = 150;

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
 */
export function useHexbins(
  bbox: BBox | null,
  zoom: number,
  filters: FilterState,
): UseHexbinsResult {
  const [hexbins, setHexbins] = useState<HexbinEntry[]>([]);
  const [loading, setLoading] = useState(false);

  const abortRef = useRef<AbortController | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const genRef = useRef(0);

  useEffect(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      abortRef.current?.abort();

      const gen = ++genRef.current;

      // Only fetch hexbins at zoom 8+
      if (!bbox || zoom < HEATMAP_MAX_ZOOM) {
        setHexbins([]);
        setLoading(false);
        return;
      }

      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);

      const qs = buildQueryString(bbox, zoom, filters);

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
    }, DEBOUNCE_MS);

    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, [bbox, zoom, filters]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  return { hexbins, loading };
}
