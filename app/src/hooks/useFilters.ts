import { useCallback, useEffect, useMemo, useState } from "react";
import type { FilterState, CategoryId } from "@/lib/types";
import { CRIME_CATEGORIES, DEFAULT_FILTERS } from "@/lib/types";

/** Maps boundary type string to the corresponding FilterState key. */
function boundaryFilterKey(type: string): keyof FilterState | null {
  switch (type) {
    case "state": return "stateFips";
    case "county": return "countyGeoids";
    case "place": return "placeGeoids";
    case "tract": return "tractGeoids";
    case "neighborhood": return "neighborhoodIds";
    default: return null;
  }
}

// -- URL serialization --

function serializeFilters(filters: FilterState): string {
  const params = new URLSearchParams();

  if (filters.categories.length > 0) {
    params.set("categories", filters.categories.join(","));
  }
  if (filters.subcategories.length > 0) {
    params.set("subcategories", filters.subcategories.join(","));
  }
  if (filters.severityMin > 1) {
    params.set("severity", String(filters.severityMin));
  }
  if (filters.datePreset) {
    params.set("date", filters.datePreset);
  }
  if (filters.arrestMade !== null) {
    params.set("arrest", String(filters.arrestMade));
  }
  if (filters.sources.length > 0) {
    params.set("sources", filters.sources.join(","));
  }

  // Boundary filters
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

  return params.toString();
}

function parseFiltersFromUrl(): FilterState {
  const params = new URLSearchParams(window.location.search);
  const filters = { ...DEFAULT_FILTERS };

  const categories = params.get("categories");
  if (categories) {
    filters.categories = categories.split(",").filter(Boolean) as CategoryId[];
  }

  const subcategories = params.get("subcategories");
  if (subcategories) {
    filters.subcategories = subcategories.split(",").filter(Boolean);
  }

  const severity = params.get("severity");
  if (severity) {
    const val = parseInt(severity, 10);
    if (val >= 1 && val <= 5) filters.severityMin = val;
  }

  const datePreset = params.get("date");
  if (datePreset) {
    filters.datePreset = datePreset;
    // Compute the actual date range from the preset
    const now = new Date();
    let from: Date;
    switch (datePreset) {
      case "7d":
        from = new Date(now.getTime() - 7 * 86400000);
        break;
      case "30d":
        from = new Date(now.getTime() - 30 * 86400000);
        break;
      case "6mo":
        from = new Date(now.getTime() - 180 * 86400000);
        break;
      case "1yr":
        from = new Date(now.getTime() - 365 * 86400000);
        break;
      default:
        from = new Date(0);
    }
    filters.dateFrom = from.toISOString();
    filters.dateTo = now.toISOString();
  }

  const arrest = params.get("arrest");
  if (arrest === "true") filters.arrestMade = true;
  else if (arrest === "false") filters.arrestMade = false;

  const sources = params.get("sources");
  if (sources) {
    filters.sources = sources
      .split(",")
      .map((s) => parseInt(s, 10))
      .filter((n) => !isNaN(n));
  }

  // Boundary filters
  const stateFips = params.get("stateFips");
  if (stateFips) filters.stateFips = stateFips.split(",").filter(Boolean);

  const countyGeoids = params.get("countyGeoids");
  if (countyGeoids) filters.countyGeoids = countyGeoids.split(",").filter(Boolean);

  const placeGeoids = params.get("placeGeoids");
  if (placeGeoids) filters.placeGeoids = placeGeoids.split(",").filter(Boolean);

  const tractGeoids = params.get("tractGeoids");
  if (tractGeoids) filters.tractGeoids = tractGeoids.split(",").filter(Boolean);

  const neighborhoodIds = params.get("neighborhoodIds");
  if (neighborhoodIds) filters.neighborhoodIds = neighborhoodIds.split(",").filter(Boolean);

  return filters;
}

// -- Hook --

/** Manages filter state with URL search param sync. */
export function useFilters() {
  const [filters, setFilters] = useState<FilterState>(parseFiltersFromUrl);

  // Sync filter state to URL on every change
  useEffect(() => {
    const search = serializeFilters(filters);
    const url = search ? `?${search}` : window.location.pathname;
    window.history.replaceState(null, "", url);
  }, [filters]);

  const toggleCategory = useCallback((id: CategoryId) => {
    setFilters((prev) => {
      const exists = prev.categories.includes(id);
      const cat = CRIME_CATEGORIES[id];
      const catSubIds = cat ? cat.subcategories.map((s) => s.id) : [];

      if (exists) {
        // Removing category — also remove all its subcategory IDs
        const catSubSet = new Set<string>(catSubIds);
        return {
          ...prev,
          categories: prev.categories.filter((c) => c !== id),
          subcategories: prev.subcategories.filter((s) => !catSubSet.has(s)),
        };
      }
      // Adding category — also add all its subcategory IDs
      const existing = new Set(prev.subcategories);
      const newSubs = catSubIds.filter((s) => !existing.has(s));
      return {
        ...prev,
        categories: [...prev.categories, id],
        subcategories: [...prev.subcategories, ...newSubs],
      };
    });
  }, []);

  const toggleSubcategory = useCallback((id: string) => {
    setFilters((prev) => {
      const exists = prev.subcategories.includes(id);
      const newSubcategories = exists
        ? prev.subcategories.filter((s) => s !== id)
        : [...prev.subcategories, id];

      // If removing a subcategory, check if any sibling subcategories
      // remain checked. If none do, also remove the parent category.
      let newCategories = prev.categories;
      if (exists) {
        const remaining = new Set(newSubcategories);
        newCategories = prev.categories.filter((catId) => {
          const cat = CRIME_CATEGORIES[catId];
          // Keep category if at least one of its subcategories is still checked
          return cat.subcategories.some((sub) => remaining.has(sub.id as string));
        });
      }

      return {
        ...prev,
        categories: newCategories,
        subcategories: newSubcategories,
      };
    });
  }, []);

  const setSeverityMin = useCallback((value: number) => {
    setFilters((prev) => ({ ...prev, severityMin: value }));
  }, []);

  const setDatePreset = useCallback((preset: string | null) => {
    if (!preset) {
      setFilters((prev) => ({
        ...prev,
        datePreset: null,
        dateFrom: null,
        dateTo: null,
      }));
      return;
    }

    const now = new Date();
    let from: Date;
    switch (preset) {
      case "7d":
        from = new Date(now.getTime() - 7 * 86400000);
        break;
      case "30d":
        from = new Date(now.getTime() - 30 * 86400000);
        break;
      case "6mo":
        from = new Date(now.getTime() - 180 * 86400000);
        break;
      case "1yr":
        from = new Date(now.getTime() - 365 * 86400000);
        break;
      default:
        from = new Date(0);
    }

    setFilters((prev) => ({
      ...prev,
      datePreset: preset,
      dateFrom: from.toISOString(),
      dateTo: now.toISOString(),
    }));
  }, []);

  const setArrestFilter = useCallback((value: boolean | null) => {
    setFilters((prev) => ({ ...prev, arrestMade: value }));
  }, []);

  const toggleSource = useCallback((sourceId: number) => {
    setFilters((prev) => {
      const exists = prev.sources.includes(sourceId);
      return {
        ...prev,
        sources: exists
          ? prev.sources.filter((s) => s !== sourceId)
          : [...prev.sources, sourceId],
      };
    });
  }, []);

  const setSources = useCallback((sourceIds: number[]) => {
    setFilters((prev) => ({ ...prev, sources: sourceIds }));
  }, []);

  /** Toggle a boundary GEOID in the corresponding filter array. */
  const toggleBoundary = useCallback((type: string, geoid: string) => {
    setFilters((prev) => {
      const key = boundaryFilterKey(type);
      if (!key) return prev;
      const arr = prev[key] as string[];
      const exists = arr.includes(geoid);
      return {
        ...prev,
        [key]: exists ? arr.filter((g) => g !== geoid) : [...arr, geoid],
      };
    });
  }, []);

  /** Clear all boundary filters of a given type. */
  const clearBoundaryFilter = useCallback((type: string) => {
    setFilters((prev) => {
      const key = boundaryFilterKey(type);
      if (!key) return prev;
      return { ...prev, [key]: [] };
    });
  }, []);

  const clearAll = useCallback(() => {
    setFilters(DEFAULT_FILTERS);
  }, []);

  const activeFilterCount = useMemo(() => {
    let count = 0;
    // Categories + subcategories are one conceptual "crime type" filter
    if (filters.categories.length > 0 || filters.subcategories.length > 0) count++;
    if (filters.severityMin > 1) count++;
    if (filters.datePreset) count++;
    if (filters.arrestMade !== null) count++;
    if (filters.sources.length > 0) count++;
    // Boundary filters — each type with selections counts as one filter
    if (filters.stateFips.length > 0) count++;
    if (filters.countyGeoids.length > 0) count++;
    if (filters.placeGeoids.length > 0) count++;
    if (filters.tractGeoids.length > 0) count++;
    if (filters.neighborhoodIds.length > 0) count++;
    return count;
  }, [filters]);

  return {
    filters,
    toggleCategory,
    toggleSubcategory,
    setSeverityMin,
    setDatePreset,
    setArrestFilter,
    toggleSource,
    setSources,
    toggleBoundary,
    clearBoundaryFilter,
    clearAll,
    activeFilterCount,
  };
}
