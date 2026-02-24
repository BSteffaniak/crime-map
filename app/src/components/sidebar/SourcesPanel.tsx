import { useState, useMemo } from "react";
import { ExternalLink, Database } from "lucide-react";
import type { ApiSource } from "@/lib/types";

interface SourcesPanelProps {
  sources: ApiSource[];
}

export default function SourcesPanel({ sources }: SourcesPanelProps) {
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    if (!search.trim()) return sources;
    const q = search.toLowerCase();
    return sources.filter(
      (s) =>
        s.name.toLowerCase().includes(q) ||
        s.city.toLowerCase().includes(q) ||
        s.state.toLowerCase().includes(q),
    );
  }, [sources, search]);

  const totalRecords = useMemo(
    () => sources.reduce((sum, s) => sum + s.recordCount, 0),
    [sources],
  );

  return (
    <div className="flex h-full flex-col bg-background">
      {/* Header */}
      <div className="border-b border-border px-4 py-3">
        <h2 className="text-lg font-semibold text-foreground">Data Sources</h2>
        <p className="text-xs text-muted-foreground">
          {sources.length} sources &middot; {totalRecords.toLocaleString()} total records
        </p>
      </div>

      {/* Search */}
      <div className="border-b border-border px-4 py-2">
        <input
          type="text"
          placeholder="Search sources..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full rounded-md border border-border bg-background px-2.5 py-1.5 text-xs text-foreground placeholder:text-muted-foreground focus:border-blue-500 focus:outline-none focus:ring-1 focus:ring-blue-500"
        />
      </div>

      {/* Source list */}
      <div className="flex-1 overflow-y-auto">
        {filtered.length === 0 && (
          <div className="px-4 py-8 text-center text-sm text-muted-foreground">
            No sources match &quot;{search}&quot;
          </div>
        )}
        {filtered.map((source) => (
          <div
            key={source.id}
            className="border-b border-border px-4 py-3 transition-colors hover:bg-accent/50"
          >
            <div className="flex items-start justify-between">
              <div className="flex items-center gap-2">
                <Database className="h-3.5 w-3.5 text-muted-foreground" />
                <span className="text-sm font-medium text-foreground">
                  {source.name}
                </span>
              </div>
              {source.portalUrl && (
                <a
                  href={source.portalUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1 text-xs text-blue-600 transition-colors hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300"
                  title="View dataset"
                >
                  Dataset
                  <ExternalLink className="h-3 w-3" />
                </a>
              )}
            </div>

            <div className="mt-1 ml-[22px] flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
              <span>{source.city}, {source.state}</span>
              <span className="tabular-nums">
                {source.recordCount.toLocaleString()} records
              </span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
