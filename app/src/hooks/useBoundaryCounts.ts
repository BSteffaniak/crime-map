/**
 * React hook for fetching per-boundary incident counts from the server.
 *
 * Queries GET /api/boundary-counts for **every** visible boundary layer
 * (not just a single "active" one). Returns a map of boundary type to
 * {geoid: count} so all visible layers can render choropleth simultaneously,
 * with more detailed layers stacking on top of coarser ones.
 *
 * Counts are fetched **without a bbox filter** so that each boundary gets
 * its true total regardless of what portion is visible in the viewport.
 * This prevents colors from changing as the user pans.
 */

import { useEffect, useRef, useState } from "react";
import type { FilterState, CategoryId } from "@/lib/types";
import { CRIME_CATEGORIES } from "@/lib/types";
import { appendBoundaryParams } from "@/lib/boundary-params";

/** geoid -> incident count */
export type BoundaryCounts = Record<string, number>;

/** All counts keyed by boundary type. */
export type AllBoundaryCounts = Partial<Record<BoundaryType, BoundaryCounts>>;

/** Boundary types. */
const BOUNDARY_TYPES = ["county", "state", "place", "tract", "neighborhood"] as const;
export type BoundaryType = (typeof BOUNDARY_TYPES)[number];

/** Choropleth display metric. */
export type BoundaryMetric = "count" | "per_capita" | "per_sq_mi";

/** Maps boundary layer toggle ID to boundary type for the API. */
const LAYER_TO_TYPE: Record<string, BoundaryType> = {
  states: "state",
  counties: "county",
  places: "place",
  tracts: "tract",
  neighborhoods: "neighborhood",
};

/** Return all visible boundary types (in order: coarsest to finest). */
function getVisibleBoundaryTypes(layers: Record<string, boolean>): BoundaryType[] {
  const order: (keyof typeof LAYER_TO_TYPE)[] = [
    "states", "counties", "places", "tracts", "neighborhoods",
  ];
  const result: BoundaryType[] = [];
  for (const layerId of order) {
    if (layers[layerId]) result.push(LAYER_TO_TYPE[layerId]);
  }
  return result;
}

function buildQueryString(
  boundaryType: BoundaryType,
  filters: FilterState,
): string {
  const params = new URLSearchParams();

  params.set("type", boundaryType);
  // No bbox — we want full boundary totals regardless of viewport

  if (filters.dateFrom) params.set("from", filters.dateFrom);
  if (filters.dateTo) params.set("to", filters.dateTo);

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

  if (filters.severityMin > 1) params.set("severityMin", String(filters.severityMin));
  if (filters.arrestMade !== null) params.set("arrestMade", String(filters.arrestMade));
  if (filters.sources.length > 0) params.set("sources", filters.sources.join(","));

  appendBoundaryParams(params, filters);

  return params.toString();
}

interface UseBoundaryCountsResult {
  /** Incident counts per boundary type, each mapping geoid -> count. */
  allBoundaryCounts: AllBoundaryCounts;
  /** Which boundary types are currently visible (coarsest to finest). */
  visibleBoundaryTypes: BoundaryType[];
  /** Whether any fetch is in progress. */
  loading: boolean;
}

/**
 * Fetches per-boundary incident counts for **all** visible boundary
 * layers in parallel. Each visible layer gets its own fetch, and results
 * are merged into a single `AllBoundaryCounts` map.
 *
 * No bbox filter — returns full totals per boundary so colors are stable
 * regardless of viewport position.
 */
export function useBoundaryCounts(
  filters: FilterState,
  layers: Record<string, boolean>,
): UseBoundaryCountsResult {
  const [allBoundaryCounts, setAllBoundaryCounts] = useState<AllBoundaryCounts>({});
  const [loading, setLoading] = useState(false);

  const abortRef = useRef<AbortController | null>(null);
  const genRef = useRef(0);

  const visibleBoundaryTypes = getVisibleBoundaryTypes(layers);
  // Stable string key so useEffect doesn't fire on every render
  const visibleKey = visibleBoundaryTypes.join(",");

  useEffect(() => {
    // No boundary layers visible — clear counts
    if (visibleBoundaryTypes.length === 0) {
      setAllBoundaryCounts({});
      setLoading(false);
      return;
    }

    abortRef.current?.abort();
    const gen = ++genRef.current;
    const controller = new AbortController();
    abortRef.current = controller;
    setLoading(true);

    // Fetch counts for every visible boundary type in parallel
    const fetches = visibleBoundaryTypes.map((boundaryType) => {
      const qs = buildQueryString(boundaryType, filters);
      return fetch(`/api/boundary-counts?${qs}`, { signal: controller.signal })
        .then((res) => {
          if (!res.ok) throw new Error(`Boundary counts API ${res.status}`);
          return res.json() as Promise<{ type: string; counts: BoundaryCounts }>;
        })
        .then((data) => ({ type: boundaryType, counts: data.counts }));
    });

    Promise.all(fetches)
      .then((results) => {
        if (gen !== genRef.current) return;
        const merged: AllBoundaryCounts = {};
        for (const { type, counts } of results) {
          merged[type] = counts;
        }
        setAllBoundaryCounts(merged);
        setLoading(false);
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        console.error("Boundary counts fetch failed:", err);
        if (gen !== genRef.current) return;
        setAllBoundaryCounts({});
        setLoading(false);
      });

    return () => {
      controller.abort();
    };
  }, [filters, visibleKey]); // eslint-disable-line react-hooks/exhaustive-deps

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  return { allBoundaryCounts, visibleBoundaryTypes, loading };
}
