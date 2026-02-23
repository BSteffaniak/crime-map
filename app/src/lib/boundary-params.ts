/**
 * Shared utility for appending boundary filter params to URLSearchParams.
 * Used by useSidebar, useHexbins, useSourceCounts, and useBoundaryCounts.
 */

import type { FilterState } from "@/lib/types";

/** Appends boundary GEOID filter params to the given URLSearchParams. */
export function appendBoundaryParams(params: URLSearchParams, filters: FilterState): void {
  if (filters.stateFips.length > 0) {
    params.set("stateFips", filters.stateFips.join(","));
  }
  if (filters.countyGeoids.length > 0) {
    params.set("countyGeoids", filters.countyGeoids.join(","));
  }
  if (filters.placeGeoids.length > 0) {
    params.set("placeGeoids", filters.placeGeoids.join(","));
  }
  if (filters.tractGeoids.length > 0) {
    params.set("tractGeoids", filters.tractGeoids.join(","));
  }
  if (filters.neighborhoodIds.length > 0) {
    params.set("neighborhoodIds", filters.neighborhoodIds.join(","));
  }
}
