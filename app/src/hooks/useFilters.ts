import { useCallback, useMemo, useState } from "react";
import type { FilterState, CategoryId } from "../lib/types";
import { DEFAULT_FILTERS } from "../lib/types";

/** Manages filter state and provides URL sync helpers. */
export function useFilters() {
  const [filters, setFilters] = useState<FilterState>(DEFAULT_FILTERS);

  const toggleCategory = useCallback((id: CategoryId) => {
    setFilters((prev) => {
      const exists = prev.categories.includes(id);
      return {
        ...prev,
        categories: exists
          ? prev.categories.filter((c) => c !== id)
          : [...prev.categories, id],
      };
    });
  }, []);

  const toggleSubcategory = useCallback((id: string) => {
    setFilters((prev) => {
      const exists = prev.subcategories.includes(id);
      return {
        ...prev,
        subcategories: exists
          ? prev.subcategories.filter((s) => s !== id)
          : [...prev.subcategories, id],
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

  const clearAll = useCallback(() => {
    setFilters(DEFAULT_FILTERS);
  }, []);

  const activeFilterCount = useMemo(() => {
    let count = 0;
    if (filters.categories.length > 0) count++;
    if (filters.subcategories.length > 0) count++;
    if (filters.severityMin > 1) count++;
    if (filters.datePreset) count++;
    if (filters.arrestMade !== null) count++;
    return count;
  }, [filters]);

  return {
    filters,
    toggleCategory,
    toggleSubcategory,
    setSeverityMin,
    setDatePreset,
    setArrestFilter,
    clearAll,
    activeFilterCount,
  };
}
