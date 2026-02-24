import { useState, useMemo } from "react";
import {
  CRIME_CATEGORIES,
  type CategoryId,
  type FilterState,
  type ApiSource,
} from "@/lib/types";
import type { SourceCounts } from "@/lib/source-counts/useSourceCounts";
import BoundaryFilter from "@/components/filters/BoundaryFilter";

const DATE_PRESETS = [
  { id: "7d", label: "7 Days" },
  { id: "30d", label: "30 Days" },
  { id: "6mo", label: "6 Months" },
  { id: "1yr", label: "1 Year" },
];

const SEVERITY_LEVELS = [
  { value: 1, label: "All" },
  { value: 2, label: "Low+" },
  { value: 3, label: "Moderate+" },
  { value: 4, label: "High+" },
  { value: 5, label: "Critical" },
];

interface FilterPanelProps {
  filters: FilterState;
  sources: ApiSource[];
  sourceCounts: SourceCounts;
  onToggleCategory: (id: CategoryId) => void;
  onToggleSubcategory: (id: string) => void;
  onSetSeverityMin: (value: number) => void;
  onSetDatePreset: (preset: string | null) => void;
  onSetArrestFilter: (value: boolean | null) => void;
  onToggleSource: (sourceId: string) => void;
  onClearSources: () => void;
  onToggleBoundary: (type: string, geoid: string) => void;
  onClearBoundaryFilter: (type: string) => void;
  onClearAll: () => void;
  activeFilterCount: number;
}

export default function FilterPanel({
  filters,
  sources,
  sourceCounts,
  onToggleCategory,
  onToggleSubcategory,
  onSetSeverityMin,
  onSetDatePreset,
  onSetArrestFilter,
  onToggleSource,
  onClearSources,
  onToggleBoundary,
  onClearBoundaryFilter,
  onClearAll,
  activeFilterCount,
}: FilterPanelProps) {
  const [sourceSearch, setSourceSearch] = useState("");

  const { pinnedSources, unpinnedSources } = useMemo(() => {
    const selectedSet = new Set(filters.sources);
    const q = sourceSearch.trim().toLowerCase();

    const matchesSearch = (s: ApiSource) =>
      !q ||
      s.name.toLowerCase().includes(q) ||
      s.city.toLowerCase().includes(q) ||
      s.state.toLowerCase().includes(q);

    // Pinned: selected sources, always shown (even if 0 in viewport after panning away)
    const pinned = sources
      .filter((s) => selectedSet.has(s.id) && matchesSearch(s))
      .sort((a, b) => (sourceCounts[b.id] ?? 0) - (sourceCounts[a.id] ?? 0));

    // Unpinned: visible-in-viewport sources that aren't selected
    const unpinned = sources
      .filter(
        (s) => !selectedSet.has(s.id) && (sourceCounts[s.id] ?? 0) > 0 && matchesSearch(s),
      )
      .sort((a, b) => (sourceCounts[b.id] ?? 0) - (sourceCounts[a.id] ?? 0));

    return { pinnedSources: pinned, unpinnedSources: unpinned };
  }, [sources, sourceCounts, sourceSearch, filters.sources]);

  return (
    <div className="flex h-full flex-col overflow-y-auto bg-background">
      {/* Header */}
      <div className="flex items-center justify-between border-b border-border px-4 py-3">
        <h2 className="text-lg font-semibold text-foreground">Filters</h2>
        {activeFilterCount > 0 && (
          <button
            onClick={onClearAll}
            className="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
          >
            Clear all ({activeFilterCount})
          </button>
        )}
      </div>

      {/* Crime Type Section */}
      <div className="border-b border-border px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-muted-foreground">Crime Type</h3>
        {(Object.keys(CRIME_CATEGORIES) as CategoryId[]).map((catId) => {
          const cat = CRIME_CATEGORIES[catId];
          const isExpanded = filters.categories.includes(catId);

          return (
            <div key={catId} className="mb-1">
              <button
                onClick={() => onToggleCategory(catId)}
                className={`flex w-full items-center gap-2 rounded px-2 py-1.5 text-left text-sm transition-colors ${
                  isExpanded
                    ? "bg-accent font-medium text-foreground"
                    : "text-muted-foreground hover:bg-accent/50"
                }`}
              >
                <span
                  className="h-3 w-3 rounded-full"
                  style={{ backgroundColor: cat.color }}
                />
                {cat.label}
              </button>

              {isExpanded && (
                <div className="ml-6 mt-1 space-y-0.5">
                  {cat.subcategories.map((sub) => {
                    const isActive = filters.subcategories.includes(sub.id);
                    return (
                      <label
                        key={sub.id}
                        className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-xs text-muted-foreground hover:bg-accent/50"
                      >
                        <input
                          type="checkbox"
                          checked={isActive}
                          onChange={() => onToggleSubcategory(sub.id)}
                          className="h-3.5 w-3.5 rounded border-border"
                        />
                        {sub.label}
                      </label>
                    );
                  })}
                </div>
              )}
            </div>
          );
        })}
      </div>

      {/* Severity Section */}
      <div className="border-b border-border px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-muted-foreground">
          Minimum Severity
        </h3>
        <div className="flex flex-wrap gap-1.5">
          {SEVERITY_LEVELS.map((level) => (
            <button
              key={level.value}
              onClick={() => onSetSeverityMin(level.value)}
              className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                filters.severityMin === level.value
                  ? "bg-foreground text-background"
                  : "bg-accent text-muted-foreground hover:bg-accent/80"
              }`}
            >
              {level.label}
            </button>
          ))}
        </div>
      </div>

      {/* Date Range Section */}
      <div className="border-b border-border px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-muted-foreground">Time Range</h3>
        <div className="flex flex-wrap gap-1.5">
          {DATE_PRESETS.map((preset) => (
            <button
              key={preset.id}
              onClick={() =>
                onSetDatePreset(
                  filters.datePreset === preset.id ? null : preset.id,
                )
              }
              className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                filters.datePreset === preset.id
                  ? "bg-foreground text-background"
                  : "bg-accent text-muted-foreground hover:bg-accent/80"
              }`}
            >
              {preset.label}
            </button>
          ))}
        </div>
      </div>

      {/* Arrest Status Section */}
      <div className="border-b border-border px-4 py-3">
        <h3 className="mb-2 text-sm font-medium text-muted-foreground">
          Arrest Status
        </h3>
        <div className="flex flex-wrap gap-1.5">
          {[
            { value: null, label: "Any" },
            { value: true, label: "Arrested" },
            { value: false, label: "No Arrest" },
          ].map((option) => (
            <button
              key={String(option.value)}
              onClick={() => onSetArrestFilter(option.value as boolean | null)}
              className={`rounded-full px-3 py-1 text-xs font-medium transition-colors ${
                filters.arrestMade === option.value
                  ? "bg-foreground text-background"
                  : "bg-accent text-muted-foreground hover:bg-accent/80"
              }`}
            >
              {option.label}
            </button>
          ))}
        </div>
      </div>

      {/* Boundary Filter Section */}
      <BoundaryFilter
        filters={filters}
        onToggleBoundary={onToggleBoundary}
        onClearBoundaryFilter={onClearBoundaryFilter}
      />

      {/* Data Source Section */}
      <div className="px-4 py-3">
        <div className="mb-2 flex items-center justify-between">
          <h3 className="text-sm font-medium text-muted-foreground">
            Data Source
          </h3>
          {filters.sources.length > 0 && (
            <button
              onClick={onClearSources}
              className="text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
            >
              Clear ({filters.sources.length})
            </button>
          )}
        </div>

        {/* Search input */}
        <input
          type="text"
          placeholder="Search sources..."
          value={sourceSearch}
          onChange={(e) => setSourceSearch(e.target.value)}
          className="mb-2 w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-xs text-foreground placeholder:text-muted-foreground focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        />

        {/* Source list */}
        <div className="max-h-48 space-y-0.5 overflow-y-auto">
          {/* Pinned: selected sources always at top */}
          {pinnedSources.map((source) => {
            const viewportCount = sourceCounts[source.id] ?? 0;
            return (
              <label
                key={source.id}
                className="flex cursor-pointer items-center gap-2 rounded bg-blue-50 px-2 py-1 text-xs text-foreground hover:bg-blue-100 dark:bg-blue-900/20 dark:hover:bg-blue-900/30"
              >
                <input
                  type="checkbox"
                  checked
                  onChange={() => onToggleSource(source.id)}
                  className="h-3.5 w-3.5 rounded border-border"
                />
                <span className="font-medium">{source.name}</span>
                <span className="ml-auto text-[10px] tabular-nums text-muted-foreground/60">
                  {viewportCount.toLocaleString()}
                </span>
              </label>
            );
          })}

          {/* Separator between pinned and unpinned */}
          {pinnedSources.length > 0 && unpinnedSources.length > 0 && (
            <div className="my-1 border-t border-border" />
          )}

          {/* Unpinned: visible-in-viewport sources */}
          {unpinnedSources.map((source) => {
            const viewportCount = sourceCounts[source.id] ?? 0;
            return (
              <label
                key={source.id}
                className="flex cursor-pointer items-center gap-2 rounded px-2 py-1 text-xs text-muted-foreground hover:bg-accent/50"
              >
                <input
                  type="checkbox"
                  checked={false}
                  onChange={() => onToggleSource(source.id)}
                  className="h-3.5 w-3.5 rounded border-border"
                />
                <span>{source.name}</span>
                <span className="ml-auto text-[10px] tabular-nums text-muted-foreground/60">
                  {viewportCount.toLocaleString()}
                </span>
              </label>
            );
          })}
          {pinnedSources.length === 0 && unpinnedSources.length === 0 && (
            <p className="px-2 py-1 text-xs text-muted-foreground/60">
              {sourceSearch.trim()
                ? <>No sources match &quot;{sourceSearch}&quot;</>
                : "No sources in the current view"}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
