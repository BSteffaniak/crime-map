import { useCallback, useEffect, useRef, useState } from "react";
import { Layers, ChevronDown } from "lucide-react";
import { MAP_LAYERS, type MapLayerConfig } from "@/lib/map-config";

interface LayerToggleProps {
  layers: Record<string, boolean>;
  zoom: number;
  onToggle: (id: string) => void;
}

/** Groups layers by their group field for sectioned display. */
function groupedLayers(): { group: string; label: string; items: MapLayerConfig[] }[] {
  return [
    {
      group: "crime",
      label: "Crime Data",
      items: MAP_LAYERS.filter((l) => l.group === "crime"),
    },
    {
      group: "boundaries",
      label: "Boundaries",
      items: MAP_LAYERS.filter((l) => l.group === "boundaries"),
    },
  ];
}

/** Expandable layer toggle button positioned over the map. */
export default function LayerToggle({ layers, zoom, onToggle }: LayerToggleProps) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const toggle = useCallback(() => setOpen((prev) => !prev), []);

  // Close on click outside
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open]);

  const groups = groupedLayers();
  const activeCount = MAP_LAYERS.filter((l) => layers[l.id]).length;

  return (
    <div ref={containerRef} className="relative">
      {/* Trigger button */}
      <button
        onClick={toggle}
        aria-label="Toggle map layers"
        aria-expanded={open}
        className="flex h-8 items-center gap-1.5 rounded-md border border-border bg-background px-2 shadow-sm transition-colors hover:bg-accent"
      >
        <Layers className="h-4 w-4 text-foreground" />
        <span className="text-xs font-medium text-foreground">Layers</span>
        {activeCount > 0 && (
          <span className="flex h-4 min-w-4 items-center justify-center rounded-full bg-blue-600 px-1 text-[10px] font-bold text-white">
            {activeCount}
          </span>
        )}
        <ChevronDown className={`h-3 w-3 text-muted-foreground transition-transform ${open ? "rotate-180" : ""}`} />
      </button>

      {/* Dropdown panel */}
      {open && (
        <div className="mt-1 w-56 overflow-hidden rounded-md border border-border bg-background shadow-lg">
          {groups.map((group) => (
            <div key={group.group}>
              <div className="border-b border-border px-3 py-1.5 text-[10px] font-semibold uppercase tracking-wider text-muted-foreground">
                {group.label}
              </div>
              {group.items.map((layer) => {
                const active = !!layers[layer.id];
                const belowMinZoom = layer.minZoom !== undefined && zoom < layer.minZoom;

                return (
                  <button
                    key={layer.id}
                    onClick={() => onToggle(layer.id)}
                    className={`flex w-full items-center gap-2.5 px-3 py-1.5 text-left text-xs transition-colors ${
                      belowMinZoom
                        ? "text-muted-foreground/50"
                        : "text-foreground hover:bg-accent/50"
                    }`}
                  >
                    {/* Toggle switch */}
                    <span
                      className={`relative inline-flex h-4 w-7 flex-shrink-0 rounded-full border transition-colors ${
                        active
                          ? "border-blue-600 bg-blue-600"
                          : "border-border bg-muted"
                      }`}
                    >
                      <span
                        className={`inline-block h-3 w-3 transform rounded-full bg-white shadow-sm transition-transform ${
                          active ? "translate-x-3" : "translate-x-0.5"
                        } mt-px`}
                      />
                    </span>
                    <span className="flex-1">{layer.label}</span>
                    {belowMinZoom && (
                      <span className="text-[10px] text-muted-foreground/60">zoom in</span>
                    )}
                  </button>
                );
              })}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
