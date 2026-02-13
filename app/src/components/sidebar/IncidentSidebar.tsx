import { useCallback, useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import { categoryColor, type FilterState } from "../../lib/types";
import { useSidebar } from "../../lib/sidebar/useSidebar";
import type { SidebarIncident } from "../../lib/sidebar/types";
import type { BBox } from "../../lib/sidebar/types";

const ESTIMATED_ROW_HEIGHT = 96;
const OVERSCAN_COUNT = 5;
/** Pixels from the bottom of the scroll container to trigger loading more. */
const LOAD_MORE_THRESHOLD = 200;

interface IncidentSidebarProps {
  bbox: BBox | null;
  filters: FilterState;
}

export default function IncidentSidebar({ bbox, filters }: IncidentSidebarProps) {
  const { features, totalCount, hasMore, loading, loadMore } =
    useSidebar(bbox, filters);

  const scrollRef = useRef<HTMLDivElement>(null);
  const loadingMoreRef = useRef(false);

  // eslint-disable-next-line react-hooks/incompatible-library
  const rowVirtualizer = useVirtualizer({
    count: features.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ESTIMATED_ROW_HEIGHT,
    overscan: OVERSCAN_COUNT,
  });

  // Infinite scroll: load more when near the bottom
  const handleScroll = useCallback(() => {
    const el = scrollRef.current;
    if (!el || !hasMore || loadingMoreRef.current) return;

    const distanceFromBottom = el.scrollHeight - el.scrollTop - el.clientHeight;
    if (distanceFromBottom < LOAD_MORE_THRESHOLD) {
      loadingMoreRef.current = true;
      loadMore();
    }
  }, [hasMore, loadMore]);

  // Reset the loadingMore flag when features change (new page arrived)
  useEffect(() => {
    loadingMoreRef.current = false;
  }, [features.length]);

  // Attach scroll listener
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    el.addEventListener("scroll", handleScroll, { passive: true });
    return () => el.removeEventListener("scroll", handleScroll);
  }, [handleScroll]);

  return (
    <div className="flex h-full flex-col bg-white">
      {/* Header */}
      <div className="border-b border-gray-200 px-4 py-3">
        <h2 className="text-lg font-semibold text-gray-900">Incidents</h2>
        <p className="text-xs text-gray-500">
          {loading
            ? "Loading..."
            : totalCount > 0
              ? `${totalCount.toLocaleString()} in view`
              : "No incidents in the current view"}
        </p>
      </div>

      {/* Virtualized Incident List */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        {features.length === 0 && !loading && (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No incidents in the current view.
            <br />
            Pan or zoom the map to see data.
          </div>
        )}

        {features.length > 0 && (
          <div
            className="relative w-full"
            style={{ height: `${rowVirtualizer.getTotalSize()}px` }}
          >
            {rowVirtualizer.getVirtualItems().map((virtualRow) => {
              const incident = features[virtualRow.index];
              return (
                <div
                  key={incident.id}
                  data-index={virtualRow.index}
                  ref={rowVirtualizer.measureElement}
                  className="absolute left-0 top-0 w-full"
                  style={{ transform: `translateY(${virtualRow.start}px)` }}
                >
                  <IncidentCard incident={incident} />
                </div>
              );
            })}
          </div>
        )}

        {/* Loading more indicator */}
        {hasMore && (
          <div className="px-4 py-3 text-center text-xs text-gray-400">
            {loadingMoreRef.current ? "Loading more..." : `${totalCount - features.length} more`}
          </div>
        )}
      </div>
    </div>
  );
}

function IncidentCard({ incident }: { incident: SidebarIncident }) {
  return (
    <div className="border-b border-gray-100 px-4 py-3 transition-colors hover:bg-gray-50">
      <div className="flex items-start justify-between">
        <div className="flex items-center gap-2">
          <span
            className="mt-0.5 h-2.5 w-2.5 rounded-full"
            style={{
              backgroundColor: categoryColor(incident.category),
            }}
          />
          <span className="text-sm font-medium text-gray-900">
            {incident.subcategory.replace(/_/g, " ")}
          </span>
        </div>
        <span className="text-xs text-gray-400">
          Sev {incident.severity}
        </span>
      </div>

      {incident.description && (
        <p className="mt-1 ml-[18px] text-xs text-gray-600 line-clamp-2">
          {incident.description}
        </p>
      )}

      <div className="mt-1 ml-[18px] flex items-center gap-3 text-xs text-gray-400">
        <span>
          {new Date(incident.occurredAt).toLocaleDateString("en-US", {
            month: "short",
            day: "numeric",
            year: "numeric",
          })}
        </span>
        {incident.blockAddress && <span>{incident.blockAddress}</span>}
        {(incident.city || incident.state) && (
          <span>
            {[incident.city, incident.state].filter(Boolean).join(", ")}
          </span>
        )}
      </div>

      {incident.arrestMade && (
        <span className="mt-1 ml-[18px] inline-block rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700">
          Arrest made
        </span>
      )}
    </div>
  );
}
