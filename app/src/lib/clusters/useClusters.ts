/**
 * React hook for fetching server-side cluster data.
 *
 * Queries GET /api/clusters with the current viewport, zoom, and filters.
 * Only active at zoom levels 8-11 (the cluster visualization range).
 * Uses the same debounce/abort pattern as useSidebar.
 */

import { useEffect, useRef, useState } from "react";
import type { FilterState, CategoryId } from "../types";
import { CRIME_CATEGORIES } from "../types";
import type { BBox } from "../sidebar/types";
import type { ClusterEntry } from "./types";
import { HEATMAP_MAX_ZOOM, CLUSTER_MAX_ZOOM } from "../map-config";

/** Debounce delay for cluster requests (ms). */
const DEBOUNCE_MS = 150;

/**
 * Builds the query string for the clusters API.
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

interface UseClustersResult {
  clusters: ClusterEntry[];
  loading: boolean;
}

/**
 * Fetches server-side cluster data for the current viewport.
 *
 * Only fetches when zoom is within the cluster range (8-11).
 * Returns an empty array outside that range.
 */
export function useClusters(
  bbox: BBox | null,
  zoom: number,
  filters: FilterState,
): UseClustersResult {
  const [clusters, setClusters] = useState<ClusterEntry[]>([]);
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

      // Only fetch clusters at zoom 8-11
      if (!bbox || zoom < HEATMAP_MAX_ZOOM || zoom >= CLUSTER_MAX_ZOOM) {
        setClusters([]);
        setLoading(false);
        return;
      }

      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);

      const qs = buildQueryString(bbox, zoom, filters);

      fetch(`/api/clusters?${qs}`, { signal: controller.signal })
        .then((res) => {
          if (!res.ok) throw new Error(`Clusters API ${res.status}`);
          return res.json() as Promise<ClusterEntry[]>;
        })
        .then((data) => {
          if (gen !== genRef.current) return;
          setClusters(data);
          setLoading(false);
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          console.error("Clusters fetch failed:", err);
          if (gen !== genRef.current) return;
          setClusters([]);
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

  return { clusters, loading };
}
