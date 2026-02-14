import { useCallback, useState } from "react";
import CrimeMap from "@/components/map/CrimeMap";
import ThemeToggle from "@/components/map/ThemeToggle";
import FilterPanel from "@/components/filters/FilterPanel";
import IncidentSidebar from "@/components/sidebar/IncidentSidebar";
import AiChat from "@/components/ai/AiChat";
import { useFilters } from "@/hooks/useFilters";
import { useTheme } from "@/hooks/useTheme";
import { useHexbins } from "@/lib/hexbins/useHexbins";
import type { BBox } from "@/lib/sidebar/types";
import { DEFAULT_ZOOM } from "@/lib/map-config";

type SidebarTab = "filters" | "incidents" | "ai";

export default function App() {
  const [sidebarTab, setSidebarTab] = useState<SidebarTab>("filters");
  const [bbox, setBbox] = useState<BBox | null>(null);
  const [zoom, setZoom] = useState(DEFAULT_ZOOM);
  const { mapTheme, cycleMapTheme } = useTheme();

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

  const { hexbins } = useHexbins(bbox, zoom, filters);

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-background text-foreground">
      {/* Left sidebar */}
      <div className="flex w-80 flex-shrink-0 flex-col border-r border-border bg-background">
        {/* Tab bar */}
        <div className="flex border-b border-border">
          <button
            onClick={() => setSidebarTab("filters")}
            className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors ${
              sidebarTab === "filters"
                ? "border-b-2 border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            Filters
            {activeFilterCount > 0 && (
              <span className="ml-1.5 inline-flex h-5 w-5 items-center justify-center rounded-full bg-blue-100 text-xs text-blue-700 dark:bg-blue-900 dark:text-blue-300">
                {activeFilterCount}
              </span>
            )}
          </button>
          <button
            onClick={() => setSidebarTab("incidents")}
            className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors ${
              sidebarTab === "incidents"
                ? "border-b-2 border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            Incidents
          </button>
          <button
            onClick={() => setSidebarTab("ai")}
            className={`flex-1 px-4 py-2.5 text-sm font-medium transition-colors ${
              sidebarTab === "ai"
                ? "border-b-2 border-blue-600 text-blue-600 dark:border-blue-400 dark:text-blue-400"
                : "text-muted-foreground hover:text-foreground"
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
      <div className="relative flex-1">
        <CrimeMap filters={filters} hexbins={hexbins} zoom={zoom} mapTheme={mapTheme} onBoundsChange={handleBoundsChange} />
        <ThemeToggle mapTheme={mapTheme} onCycle={cycleMapTheme} />
      </div>
    </div>
  );
}
