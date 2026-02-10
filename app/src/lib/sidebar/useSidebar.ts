/**
 * React hook for fetching sidebar data from the server API.
 *
 * Replaces the old FlatGeobuf web worker approach with simple HTTP
 * requests to GET /api/sidebar. Supports debouncing, request
 * cancellation via AbortController, and infinite scroll pagination.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import type { FilterState, CategoryId } from "../types";
import { CRIME_CATEGORIES } from "../types";
import type { BBox, SidebarIncident, SidebarResponse } from "./types";

/** Debounce delay for sidebar requests (ms). */
const DEBOUNCE_MS = 150;

/** Number of features per page. */
const PAGE_SIZE = 50;

interface UseSidebarResult {
  features: SidebarIncident[];
  totalCount: number;
  hasMore: boolean;
  loading: boolean;
  loadMore: () => void;
}

/**
 * Builds the query string for the sidebar API from the current
 * bbox, filters, limit, and offset.
 */
function buildQueryString(
  bbox: BBox | null,
  filters: FilterState,
  limit: number,
  offset: number,
): string {
  const params = new URLSearchParams();

  if (bbox) {
    params.set("bbox", bbox.join(","));
  }

  params.set("limit", String(limit));
  params.set("offset", String(offset));

  // Date filters
  if (filters.dateFrom) {
    params.set("from", filters.dateFrom);
  }
  if (filters.dateTo) {
    params.set("to", filters.dateTo);
  }

  // Category / subcategory filters
  // If subcategories are explicitly selected, send those.
  // If only categories are selected, expand to all their subcategories.
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

/**
 * Fetches paginated sidebar data from the server API.
 *
 * @param bbox - Current viewport bounding box (null if not yet known)
 * @param filters - Active filter state
 */
export function useSidebar(
  bbox: BBox | null,
  filters: FilterState,
): UseSidebarResult {
  const [features, setFeatures] = useState<SidebarIncident[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [loading, setLoading] = useState(false);

  const abortRef = useRef<AbortController | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Track the "generation" of the current query so pagination doesn't
  // mix results from different bbox/filter combos.
  const genRef = useRef(0);

  // Re-fetch when bbox or filters change (debounced)
  useEffect(() => {
    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      // Cancel any in-flight request
      abortRef.current?.abort();

      const gen = ++genRef.current;

      if (!bbox) {
        setFeatures([]);
        setTotalCount(0);
        setHasMore(false);
        setLoading(false);
        return;
      }

      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);

      const qs = buildQueryString(bbox, filters, PAGE_SIZE, 0);

      fetch(`/api/sidebar?${qs}`, { signal: controller.signal })
        .then((res) => {
          if (!res.ok) throw new Error(`Sidebar API ${res.status}`);
          return res.json() as Promise<SidebarResponse>;
        })
        .then((data) => {
          if (gen !== genRef.current) return; // stale
          setFeatures(data.features);
          setTotalCount(data.totalCount);
          setHasMore(data.hasMore);
          setLoading(false);
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          console.error("Sidebar fetch failed:", err);
          if (gen !== genRef.current) return;
          setLoading(false);
        });
    }, DEBOUNCE_MS);

    return () => {
      if (debounceRef.current) {
        clearTimeout(debounceRef.current);
      }
    };
  }, [bbox, filters]);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  const loadMore = useCallback(() => {
    if (!bbox || loading) return;

    const gen = genRef.current;
    const offset = features.length;

    const controller = new AbortController();
    // Don't abort the main controller — this is a pagination request
    const qs = buildQueryString(bbox, filters, PAGE_SIZE, offset);

    fetch(`/api/sidebar?${qs}`, { signal: controller.signal })
      .then((res) => {
        if (!res.ok) throw new Error(`Sidebar API ${res.status}`);
        return res.json() as Promise<SidebarResponse>;
      })
      .then((data) => {
        if (gen !== genRef.current) return; // stale
        setFeatures((prev) => [...prev, ...data.features]);
        setHasMore(data.hasMore);
        // totalCount shouldn't change between pages
      })
      .catch((err) => {
        if (err instanceof DOMException && err.name === "AbortError") return;
        console.error("Sidebar pagination failed:", err);
      });
  }, [bbox, filters, features.length, loading]);

  return { features, totalCount, hasMore, loading, loadMore };
}
