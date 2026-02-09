/**
 * JS filter predicates for filtering crime incidents in the web worker.
 *
 * These mirror the MapLibre filter expressions but run as plain JS functions
 * against CrimePoint objects loaded from FlatGeobuf.
 */

import type { FilterState } from "../types";
import { CRIME_CATEGORIES } from "../types";
import type { CrimePoint } from "../cluster-worker/types";

/** Builds a predicate function from the current filter state. */
export function buildIncidentPredicate(
  filters: FilterState,
): (point: CrimePoint) => boolean {
  // Pre-compute the effective subcategory set for fast lookup.
  // If subcategories are explicitly selected, use those.
  // If only categories are selected (expanded), include all their subcategories.
  // If neither, pass everything.
  let subcategorySet: Set<string> | null = null;

  if (filters.subcategories.length > 0) {
    subcategorySet = new Set(filters.subcategories);
  } else if (filters.categories.length > 0) {
    subcategorySet = new Set<string>();
    for (const catId of filters.categories) {
      const cat = CRIME_CATEGORIES[catId];
      if (cat) {
        for (const sub of cat.subcategories) {
          subcategorySet.add(sub.id);
        }
      }
    }
  }

  const { severityMin, dateFrom, dateTo, arrestMade } = filters;

  return (point: CrimePoint): boolean => {
    // Subcategory / category filter
    if (subcategorySet !== null && !subcategorySet.has(point.subcategory)) {
      return false;
    }

    // Severity filter
    if (severityMin > 1 && point.severity < severityMin) {
      return false;
    }

    // Date range filter (ISO string comparison works for chronological order)
    if (dateFrom !== null && point.date < dateFrom) {
      return false;
    }
    if (dateTo !== null && point.date > dateTo) {
      return false;
    }

    // Arrest filter
    if (arrestMade !== null && point.arrest !== arrestMade) {
      return false;
    }

    return true;
  };
}
