import { useCallback, useState } from "react";
import CrimeMap from "./components/map/CrimeMap";
import FilterPanel from "./components/filters/FilterPanel";
import IncidentSidebar from "./components/sidebar/IncidentSidebar";
import AiChat from "./components/ai/AiChat";
import { useFilters } from "./hooks/useFilters";
import { useClusters } from "./lib/clusters/useClusters";
import type { BBox } from "./lib/sidebar/types";
import { DEFAULT_ZOOM } from "./lib/map-config";

type SidebarTab = "filters" | "incidents" | "ai";

export default function App() {
  const [sidebarTab, setSidebarTab] = useState<SidebarTab>("filters");
  const [bbox, setBbox] = useState<BBox | null>(null);
  const [zoom, setZoom] = useState(DEFAULT_ZOOM);

  const {
    filters,
    toggleCategory,
    toggleSubcategory,
    setSeverityMin,
    setDatePreset,
    setArrestFilter,
    clearAll,
    activeFilterCount,
  } = useFilters();

  const handleBoundsChange = useCallback(
    (bounds: { getWest(): number; getSouth(): number; getEast(): number; getNorth(): number }, newZoom: number) => {
      setBbox([
        bounds.getWest(),
        bounds.getSouth(),
        bounds.getEast(),
        bounds.getNorth(),
      ]);
      setZoom(newZoom);
    },
    [],
  );

  const { clusters } = useClusters(bbox, zoom, filters);

  return (
    <div className="flex h-screen w-screen overflow-hidden">
      {/* Left sidebar */}
      <div className="flex w-80 flex-shrink-0 flex-col border-r border-gray-200">
        {/* Tab bar */}
        <div className="flex border-b border-gray-200">
          <button
            onClick={() => setSidebarTab("filters")}
            className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors ${
              sidebarTab === "filters"
                ? "border-b-2 border-blue-600 text-blue-600"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            Filters
            {activeFilterCount > 0 && (
              <span className="ml-1.5 inline-flex h-5 w-5 items-center justify-center rounded-full bg-blue-100 text-xs text-blue-700">
                {activeFilterCount}
              </span>
            )}
          </button>
          <button
            onClick={() => setSidebarTab("incidents")}
            className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors ${
              sidebarTab === "incidents"
                ? "border-b-2 border-blue-600 text-blue-600"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            Incidents
          </button>
          <button
            onClick={() => setSidebarTab("ai")}
            className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors ${
              sidebarTab === "ai"
                ? "border-b-2 border-blue-600 text-blue-600"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            Ask AI
          </button>
        </div>

        {/* Tab content */}
        <div className="flex-1 overflow-hidden">
          {sidebarTab === "filters" ? (
            <FilterPanel
              filters={filters}
              onToggleCategory={toggleCategory}
              onToggleSubcategory={toggleSubcategory}
              onSetSeverityMin={setSeverityMin}
              onSetDatePreset={setDatePreset}
              onSetArrestFilter={setArrestFilter}
              onClearAll={clearAll}
              activeFilterCount={activeFilterCount}
            />
          ) : sidebarTab === "incidents" ? (
            <IncidentSidebar bbox={bbox} filters={filters} />
          ) : (
            <AiChat />
          )}
        </div>
      </div>

      {/* Map */}
      <div className="flex-1">
        <CrimeMap filters={filters} clusters={clusters} onBoundsChange={handleBoundsChange} />
      </div>
    </div>
  );
}
