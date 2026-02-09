import { useCallback, useState } from "react";
import type maplibregl from "maplibre-gl";
import CrimeMap from "./components/map/CrimeMap";
import FilterPanel from "./components/filters/FilterPanel";
import IncidentSidebar from "./components/sidebar/IncidentSidebar";
import { useFilters } from "./hooks/useFilters";

export default function App() {
  const [bounds, setBounds] = useState<maplibregl.LngLatBounds | null>(null);
  const [sidebarTab, setSidebarTab] = useState<"filters" | "incidents">(
    "filters",
  );

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

  const handleBoundsChange = useCallback((newBounds: maplibregl.LngLatBounds) => {
    setBounds(newBounds);
  }, []);

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
          ) : (
            <IncidentSidebar bounds={bounds} />
          )}
        </div>
      </div>

      {/* Map */}
      <div className="flex-1">
        <CrimeMap onBoundsChange={handleBoundsChange} />
      </div>
    </div>
  );
}
