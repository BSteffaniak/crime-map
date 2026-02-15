import { useCallback, useRef, useState } from "react";
import { SlidersHorizontal, X } from "lucide-react";
import CrimeMap from "@/components/map/CrimeMap";
import ThemeToggle from "@/components/map/ThemeToggle";
import FilterPanel from "@/components/filters/FilterPanel";
import IncidentSidebar from "@/components/sidebar/IncidentSidebar";
import AiChat from "@/components/ai/AiChat";
import SidebarPanel from "@/components/sidebar/SidebarPanel";
import { useFilters } from "@/hooks/useFilters";
import { useTheme } from "@/hooks/useTheme";
import { useHexbins } from "@/lib/hexbins/useHexbins";
import type { BBox } from "@/lib/sidebar/types";
import { DEFAULT_ZOOM } from "@/lib/map-config";

type SidebarTab = "filters" | "incidents" | "ai";

export default function App() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarTab, setSidebarTab] = useState<SidebarTab>("filters");
  const [bbox, setBbox] = useState<BBox | null>(null);
  const [zoom, setZoom] = useState(DEFAULT_ZOOM);
  const settledRef = useRef(true);
  const { mapTheme, setMapTheme } = useTheme();

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
    (bounds: { getWest(): number; getSouth(): number; getEast(): number; getNorth(): number }, newZoom: number, options: { settled: boolean }) => {
      settledRef.current = options.settled;
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

  const { hexbins } = useHexbins(bbox, zoom, filters, settledRef);

  return (
    <div className="relative h-dvh w-screen overflow-hidden bg-background text-foreground">
      {/* Map — always full viewport */}
      <CrimeMap filters={filters} hexbins={hexbins} zoom={zoom} mapTheme={mapTheme} onBoundsChange={handleBoundsChange} />

      {/* Floating controls — top-left */}
      <div className="absolute top-3 left-2 z-10 flex items-start gap-2">
        {/* Sidebar toggle button */}
        {!sidebarOpen && (
          <button
            onClick={() => setSidebarOpen(true)}
            aria-label="Open filters panel"
            className="relative flex h-8 items-center gap-1.5 rounded-md border border-border bg-background px-2 shadow-sm transition-colors hover:bg-accent"
          >
            <SlidersHorizontal className="h-4 w-4 text-foreground" />
            <span className="text-xs font-medium text-foreground">Filters</span>
            {activeFilterCount > 0 && (
              <span className="absolute -top-1.5 -right-1.5 flex h-4 min-w-4 items-center justify-center rounded-full bg-blue-600 px-1 text-[10px] font-bold text-white">
                {activeFilterCount}
              </span>
            )}
          </button>
        )}

        <ThemeToggle mapTheme={mapTheme} onSelect={setMapTheme} />
      </div>

      {/* Floating sidebar panel */}
      <SidebarPanel open={sidebarOpen} onClose={() => setSidebarOpen(false)}>
        {/* Tab bar */}
        <div className="flex flex-shrink-0 border-b border-border">
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
          <button
            onClick={() => setSidebarOpen(false)}
            className="flex items-center justify-center px-3 text-muted-foreground transition-colors hover:text-foreground"
            aria-label="Close panel"
          >
            <X className="h-4 w-4" />
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
            <IncidentSidebar bbox={bbox} filters={filters} settledRef={settledRef} />
          ) : (
            <AiChat />
          )}
        </div>
      </SidebarPanel>
    </div>
  );
}
