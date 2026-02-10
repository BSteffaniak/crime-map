import { categoryColor, type FilterState } from "../../lib/types";
import { useSidebarWorker } from "../../lib/cluster-worker";
import type { SidebarFeature } from "../../lib/cluster-worker/types";

interface IncidentSidebarProps {
  filters: FilterState;
}

export default function IncidentSidebar({ filters }: IncidentSidebarProps) {
  const { sidebarFeatures, totalCount, loading, loadMore } =
    useSidebarWorker(filters);

  const hasMore = sidebarFeatures.length < totalCount;

  return (
    <div className="flex h-full flex-col bg-white">
      {/* Header */}
      <div className="border-b border-gray-200 px-4 py-3">
        <h2 className="text-lg font-semibold text-gray-900">Incidents</h2>
        <p className="text-xs text-gray-500">
          {loading ? "Loading..." : `${totalCount} in view`}
        </p>
      </div>

      {/* Incident List */}
      <div className="flex-1 overflow-y-auto">
        {sidebarFeatures.length === 0 && !loading && (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No incidents in the current view.
            <br />
            Pan or zoom the map to see data.
          </div>
        )}

        {sidebarFeatures.map((incident) => (
          <IncidentCard key={incident.id} incident={incident} />
        ))}

        {hasMore && (
          <button
            onClick={loadMore}
            className="w-full border-t border-gray-100 px-4 py-3 text-center text-sm text-blue-600 hover:bg-gray-50"
          >
            Load more ({totalCount - sidebarFeatures.length} remaining)
          </button>
        )}
      </div>
    </div>
  );
}

function IncidentCard({ incident }: { incident: SidebarFeature }) {
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

      {incident.desc && (
        <p className="mt-1 ml-[18px] text-xs text-gray-600 line-clamp-2">
          {incident.desc}
        </p>
      )}

      <div className="mt-1 ml-[18px] flex items-center gap-3 text-xs text-gray-400">
        <span>
          {new Date(incident.date).toLocaleDateString("en-US", {
            month: "short",
            day: "numeric",
            year: "numeric",
          })}
        </span>
        {incident.addr && <span>{incident.addr}</span>}
        <span>
          {incident.city}, {incident.state}
        </span>
      </div>

      {incident.arrest && (
        <span className="mt-1 ml-[18px] inline-block rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700">
          Arrest made
        </span>
      )}
    </div>
  );
}
