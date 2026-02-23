/**
 * React hook for fetching per-source incident counts from the server.
 *
 * Queries GET /api/source-counts with the current viewport and filters.
 * Returns a Record<number, number> mapping source_id to viewport count.
 * Only sources with >0 incidents in the viewport are included.
 *
 * Fetch scheduling matches the hexbins/sidebar pattern:
 * - On `moveend` (settled=true): fetches immediately, no debounce.
 * - On mid-pan `move` (settled=false): debounced by VIEWPORT_DEBOUNCE_MS.
 */

import { type MutableRefObject, useEffect, useRef, useState } from "react";
import type { FilterState, CategoryId } from "../types";
import { CRIME_CATEGORIES } from "../types";
import type { BBox } from "../sidebar/types";
import { VIEWPORT_DEBOUNCE_MS } from "../map-config";
import { appendBoundaryParams } from "../boundary-params";

/** source_id -> viewport incident count */
export type SourceCounts = Record<number, number>;

/**
 * Builds the query string for the source-counts API.
 */
function buildQueryString(bbox: BBox, filters: FilterState): string {
  const params = new URLSearchParams();

  params.set("bbox", bbox.join(","));

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

  // NOTE: We intentionally do NOT pass the sources filter here.
  // We want counts for ALL sources in the viewport so the user can
  // see which sources are available to filter by.

  // Boundary filters
  appendBoundaryParams(params, filters);

  return params.toString();
}

interface UseSourceCountsResult {
  sourceCounts: SourceCounts;
  loading: boolean;
}

/**
 * Fetches per-source incident counts for the current viewport.
 *
 * Used by the filter panel to show only sources visible in the viewport
 * and their viewport-scoped record counts.
 */
export function useSourceCounts(
  bbox: BBox | null,
  filters: FilterState,
  settledRef: MutableRefObject<boolean>,
): UseSourceCountsResult {
  const [sourceCounts, setSourceCounts] = useState<SourceCounts>({});
  const [loading, setLoading] = useState(false);

  const abortRef = useRef<AbortController | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const genRef = useRef(0);

  useEffect(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
      debounceRef.current = null;
    }

    const settled = settledRef.current;

    const doFetch = () => {
      abortRef.current?.abort();

      const gen = ++genRef.current;

      if (!bbox) {
        setSourceCounts({});
        setLoading(false);
        return;
      }

      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);

      const qs = buildQueryString(bbox, filters);

      fetch(`/api/source-counts?${qs}`, { signal: controller.signal })
        .then((res) => {
          if (!res.ok) throw new Error(`Source counts API ${res.status}`);
          return res.json() as Promise<SourceCounts>;
        })
        .then((data) => {
          if (gen !== genRef.current) return;
          setSourceCounts(data);
          setLoading(false);
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          console.error("Source counts fetch failed:", err);
          if (gen !== genRef.current) return;
          setLoading(false);
        });
    };

    if (settled) {
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
  }, [bbox, filters, settledRef]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  return { sourceCounts, loading };
}
