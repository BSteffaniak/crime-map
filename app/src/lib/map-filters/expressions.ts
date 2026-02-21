/**
 * Builds MapLibre filter expressions from FilterState.
 *
 * Applied to PMTiles vector tile layers for client-side filtering at high zoom.
 * The logic mirrors the server-side SQL filters in the sidebar endpoint.
 */

import type { FilterSpecification } from "maplibre-gl";
import type { FilterState } from "../types";
import { CRIME_CATEGORIES } from "../types";

/** Builds a MapLibre filter expression for the incidents layers. */
export function buildIncidentFilter(
  filters: FilterState,
): FilterSpecification | null {
  const clauses: FilterSpecification[] = [];

  // Subcategory / category filter
  if (filters.subcategories.length > 0) {
    clauses.push([
      "in",
      ["get", "subcategory"],
      ["literal", filters.subcategories],
    ]);
  } else if (filters.categories.length > 0) {
    // Expand selected categories into their subcategory IDs
    const allSubs: string[] = [];
    for (const catId of filters.categories) {
      const cat = CRIME_CATEGORIES[catId];
      if (cat) {
        for (const sub of cat.subcategories) {
          allSubs.push(sub.id);
        }
      }
    }
    if (allSubs.length > 0) {
      clauses.push(["in", ["get", "subcategory"], ["literal", allSubs]]);
    }
  }

  // Severity filter
  if (filters.severityMin > 1) {
    clauses.push([">=", ["get", "severity"], filters.severityMin]);
  }

  // Date range filter (string comparison on ISO dates)
  if (filters.dateFrom !== null) {
    clauses.push([">=", ["get", "date"], filters.dateFrom]);
  }
  if (filters.dateTo !== null) {
    clauses.push(["<=", ["get", "date"], filters.dateTo]);
  }

  // Arrest filter
  if (filters.arrestMade === true) {
    clauses.push(["==", ["get", "arrest"], true]);
  } else if (filters.arrestMade === false) {
    clauses.push(["==", ["get", "arrest"], false]);
  }

  // Source filter
  if (filters.sources.length > 0) {
    clauses.push(["in", ["get", "src"], ["literal", filters.sources]]);
  }

  if (clauses.length === 0) return null;
  if (clauses.length === 1) return clauses[0];
  return ["all", ...clauses] as FilterSpecification;
}
