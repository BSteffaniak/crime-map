import { useCallback, useEffect, useState } from "react";
import type { ApiIncident } from "../../lib/types";
import { categoryColor } from "../../lib/types";

interface IncidentSidebarProps {
  bounds: maplibregl.LngLatBounds | null;
}

export default function IncidentSidebar({ bounds }: IncidentSidebarProps) {
  const [incidents, setIncidents] = useState<ApiIncident[]>([]);
  const [loading, setLoading] = useState(false);
  const [total, setTotal] = useState(0);

  const fetchIncidents = useCallback(async () => {
    if (!bounds) return;

    setLoading(true);
    try {
      const bbox = [
        bounds.getWest(),
        bounds.getSouth(),
        bounds.getEast(),
        bounds.getNorth(),
      ].join(",");

      const response = await fetch(
        `/api/incidents?bbox=${bbox}&limit=50&offset=0`,
      );
      if (!response.ok) throw new Error("Failed to fetch");

      const data: ApiIncident[] = await response.json();
      setIncidents(data);
      setTotal(data.length);
    } catch (err) {
      console.error("Failed to fetch incidents:", err);
    } finally {
      setLoading(false);
    }
  }, [bounds]);

  useEffect(() => {
    fetchIncidents();
  }, [fetchIncidents]);

  return (
    <div className="flex h-full flex-col bg-white">
      {/* Header */}
      <div className="border-b border-gray-200 px-4 py-3">
        <h2 className="text-lg font-semibold text-gray-900">Incidents</h2>
        <p className="text-xs text-gray-500">
          {loading ? "Loading..." : `${total} in view`}
        </p>
      </div>

      {/* Incident List */}
      <div className="flex-1 overflow-y-auto">
        {incidents.length === 0 && !loading && (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No incidents in the current view.
            <br />
            Pan or zoom the map to see data.
          </div>
        )}

        {incidents.map((incident) => (
          <div
            key={incident.id}
            className="border-b border-gray-100 px-4 py-3 transition-colors hover:bg-gray-50"
          >
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
                Sev {incident.severityValue}
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
              <span>
                {incident.city}, {incident.state}
              </span>
            </div>

            {incident.arrestMade && (
              <span className="mt-1 ml-[18px] inline-block rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700">
                Arrest made
              </span>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
