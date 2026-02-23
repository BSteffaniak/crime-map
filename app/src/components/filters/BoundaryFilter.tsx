import { useCallback, useEffect, useRef, useState } from "react";
import type { FilterState } from "@/lib/types";

/** Boundary search result from GET /api/boundaries/search. */
interface BoundarySearchResult {
  geoid: string;
  name: string;
  fullName: string | null;
  stateAbbr: string | null;
  population: number | null;
  type: string;
}

/** All boundary types with display labels. */
const BOUNDARY_TYPES = [
  { id: "state", label: "States", filterKey: "stateFips" as const },
  { id: "county", label: "Counties", filterKey: "countyGeoids" as const },
  { id: "place", label: "Places", filterKey: "placeGeoids" as const },
  { id: "tract", label: "Tracts", filterKey: "tractGeoids" as const },
  { id: "neighborhood", label: "Neighborhoods", filterKey: "neighborhoodIds" as const },
] as const;

type BoundaryTypeId = (typeof BOUNDARY_TYPES)[number]["id"];

/** Maps a boundary type to its FilterState key. */
function filterKeyForType(type: string): keyof FilterState | null {
  const entry = BOUNDARY_TYPES.find((bt) => bt.id === type);
  return entry?.filterKey ?? null;
}

/** Gets the display label for a boundary type. */
function labelForType(type: string): string {
  return BOUNDARY_TYPES.find((bt) => bt.id === type)?.label ?? type;
}

/** Formats a population number compactly. */
function formatPop(pop: number): string {
  if (pop >= 1_000_000) return `${(pop / 1_000_000).toFixed(1)}M`;
  if (pop >= 1_000) return `${(pop / 1_000).toFixed(1)}K`;
  return String(pop);
}

interface BoundaryFilterProps {
  filters: FilterState;
  onToggleBoundary: (type: string, geoid: string) => void;
  onClearBoundaryFilter: (type: string) => void;
}

export default function BoundaryFilter({
  filters,
  onToggleBoundary,
  onClearBoundaryFilter,
}: BoundaryFilterProps) {
  const [query, setQuery] = useState("");
  const [typeFilter, setTypeFilter] = useState<BoundaryTypeId | "all">("all");
  const [results, setResults] = useState<BoundarySearchResult[]>([]);
  const [loading, setLoading] = useState(false);
  const [showResults, setShowResults] = useState(false);

  /** Cache of geoid -> display name, populated from search results. */
  const nameCache = useRef<Record<string, string>>({});

  const abortRef = useRef<AbortController | null>(null);
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Count total selected boundaries
  const totalSelected =
    filters.stateFips.length +
    filters.countyGeoids.length +
    filters.placeGeoids.length +
    filters.tractGeoids.length +
    filters.neighborhoodIds.length;

  // Search boundaries when query changes
  const doSearch = useCallback(
    (q: string, type: BoundaryTypeId | "all") => {
      abortRef.current?.abort();

      if (q.trim().length < 2) {
        setResults([]);
        setLoading(false);
        return;
      }

      const controller = new AbortController();
      abortRef.current = controller;
      setLoading(true);

      const params = new URLSearchParams();
      params.set("q", q.trim());
      if (type !== "all") params.set("type", type);
      params.set("limit", "20");

      fetch(`/api/boundaries/search?${params}`, { signal: controller.signal })
        .then((res) => {
          if (!res.ok) throw new Error(`Search API ${res.status}`);
          return res.json() as Promise<BoundarySearchResult[]>;
        })
        .then((data) => {
          // Cache display names for selected-chip labels
          for (const r of data) {
            const key = `${r.type}-${r.geoid}`;
            nameCache.current[key] = r.fullName ?? r.name;
          }
          setResults(data);
          setLoading(false);
        })
        .catch((err) => {
          if (err instanceof DOMException && err.name === "AbortError") return;
          console.error("Boundary search failed:", err);
          setResults([]);
          setLoading(false);
        });
    },
    [],
  );

  // Debounced search on query/type change
  useEffect(() => {
    if (debounceRef.current) clearTimeout(debounceRef.current);
    debounceRef.current = setTimeout(() => doSearch(query, typeFilter), 250);
    return () => {
      if (debounceRef.current) clearTimeout(debounceRef.current);
    };
  }, [query, typeFilter, doSearch]);

  // Close results on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (
        containerRef.current &&
        !containerRef.current.contains(e.target as Node)
      ) {
        setShowResults(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, []);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  /** Check if a geoid is already selected for its boundary type. */
  const isSelected = (type: string, geoid: string): boolean => {
    const key = filterKeyForType(type);
    if (!key) return false;
    return (filters[key] as string[]).includes(geoid);
  };

  /** Collect all selected boundaries as [type, geoid, name] tuples. */
  const selectedBoundaries: { type: string; geoid: string; label: string }[] =
    [];
  for (const bt of BOUNDARY_TYPES) {
    const arr = filters[bt.filterKey] as string[];
    for (const geoid of arr) {
      const cacheKey = `${bt.id}-${geoid}`;
      selectedBoundaries.push({
        type: bt.id,
        geoid,
        label: nameCache.current[cacheKey] ?? geoid,
      });
    }
  }

  return (
    <div className="border-b border-border px-4 py-3" ref={containerRef}>
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-medium text-muted-foreground">
          Boundary
        </h3>
        {totalSelected > 0 && (
          <button
            onClick={() => {
              for (const bt of BOUNDARY_TYPES) {
                if ((filters[bt.filterKey] as string[]).length > 0) {
                  onClearBoundaryFilter(bt.id);
                }
              }
            }}
            className="text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
          >
            Clear ({totalSelected})
          </button>
        )}
      </div>

      {/* Selected boundary chips */}
      {selectedBoundaries.length > 0 && (
        <div className="mb-2 flex flex-wrap gap-1">
          {selectedBoundaries.map(({ type, geoid, label }) => (
            <button
              key={`${type}-${geoid}`}
              onClick={() => onToggleBoundary(type, geoid)}
              className="inline-flex items-center gap-1 rounded-full bg-blue-100 px-2 py-0.5 text-[10px] font-medium text-blue-800 transition-colors hover:bg-blue-200 dark:bg-blue-900/30 dark:text-blue-300 dark:hover:bg-blue-900/50"
              title={`Remove ${label} (${geoid})`}
            >
              <span className="text-[9px] text-muted-foreground/60">
                {labelForType(type).slice(0, -1)}
              </span>
              {label}
              <span className="ml-0.5 text-blue-500">&times;</span>
            </button>
          ))}
        </div>
      )}

      {/* Type filter tabs */}
      <div className="mb-1.5 flex flex-wrap gap-1">
        <button
          onClick={() => setTypeFilter("all")}
          className={`rounded-full px-2 py-0.5 text-[10px] font-medium transition-colors ${
            typeFilter === "all"
              ? "bg-foreground text-background"
              : "bg-accent text-muted-foreground hover:bg-accent/80"
          }`}
        >
          All
        </button>
        {BOUNDARY_TYPES.map((bt) => (
          <button
            key={bt.id}
            onClick={() => setTypeFilter(bt.id)}
            className={`rounded-full px-2 py-0.5 text-[10px] font-medium transition-colors ${
              typeFilter === bt.id
                ? "bg-foreground text-background"
                : "bg-accent text-muted-foreground hover:bg-accent/80"
            }`}
          >
            {bt.label}
          </button>
        ))}
      </div>

      {/* Search input */}
      <div className="relative">
        <input
          type="text"
          placeholder="Search boundaries..."
          value={query}
          onChange={(e) => {
            setQuery(e.target.value);
            setShowResults(true);
          }}
          onFocus={() => setShowResults(true)}
          className="w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-xs text-foreground placeholder:text-muted-foreground focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        />

        {/* Results dropdown */}
        {showResults && query.trim().length >= 2 && (
          <div className="absolute left-0 right-0 top-full z-20 mt-1 max-h-48 overflow-y-auto rounded-md border border-border bg-background shadow-lg">
            {loading && results.length === 0 && (
              <div className="px-3 py-2 text-xs text-muted-foreground">
                Searching...
              </div>
            )}
            {!loading && results.length === 0 && (
              <div className="px-3 py-2 text-xs text-muted-foreground">
                No boundaries match &quot;{query}&quot;
              </div>
            )}
            {results.map((result) => {
              const selected = isSelected(result.type, result.geoid);
              return (
                <button
                  key={`${result.type}-${result.geoid}`}
                  onClick={() => {
                    onToggleBoundary(result.type, result.geoid);
                  }}
                  className={`flex w-full items-center gap-2 px-3 py-1.5 text-left text-xs transition-colors ${
                    selected
                      ? "bg-blue-50 text-blue-800 dark:bg-blue-900/20 dark:text-blue-300"
                      : "text-foreground hover:bg-accent/50"
                  }`}
                >
                  <input
                    type="checkbox"
                    checked={selected}
                    readOnly
                    className="h-3 w-3 rounded border-border"
                  />
                  <div className="min-w-0 flex-1">
                    <div className="truncate font-medium">
                      {result.fullName ?? result.name}
                      {result.stateAbbr && !result.fullName && (
                        <span className="text-muted-foreground">
                          , {result.stateAbbr}
                        </span>
                      )}
                    </div>
                    <div className="flex items-center gap-1 text-[10px] text-muted-foreground/60">
                      <span>{labelForType(result.type).slice(0, -1)}</span>
                      {result.population && (
                        <>
                          <span>&middot;</span>
                          <span>Pop. {formatPop(result.population)}</span>
                        </>
                      )}
                      <span>&middot;</span>
                      <span>{result.geoid}</span>
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}
