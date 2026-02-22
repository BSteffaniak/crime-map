import { useEffect } from "react";
import { useMap } from "@/components/ui/map";
import { BOUNDARIES_PMTILES_URL, LABEL_BEFORE_ID } from "@/lib/map-config";

/** Boundary layer definitions with style properties. */
const BOUNDARY_LAYER_DEFS = [
  {
    id: "states",
    sourceLayer: "states",
    minzoom: 0,
    fillColor: { light: "rgba(100, 116, 139, 0.04)", dark: "rgba(148, 163, 184, 0.06)" },
    lineColor: { light: "#64748b", dark: "#94a3b8" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      0, 1,
      6, 1.5,
      10, 2,
    ],
    lineOpacity: 0.6,
  },
  {
    id: "counties",
    sourceLayer: "counties",
    minzoom: 4,
    fillColor: { light: "rgba(100, 116, 139, 0.03)", dark: "rgba(148, 163, 184, 0.04)" },
    lineColor: { light: "#94a3b8", dark: "#64748b" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      4, 0.4,
      8, 0.8,
      14, 1.2,
    ],
    lineOpacity: 0.5,
  },
  {
    id: "places",
    sourceLayer: "places",
    minzoom: 7,
    fillColor: { light: "rgba(59, 130, 246, 0.04)", dark: "rgba(96, 165, 250, 0.05)" },
    lineColor: { light: "#3b82f6", dark: "#60a5fa" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      7, 0.4,
      10, 0.8,
      14, 1,
    ],
    lineOpacity: 0.5,
  },
  {
    id: "tracts",
    sourceLayer: "tracts",
    minzoom: 9,
    fillColor: { light: "rgba(168, 85, 247, 0.03)", dark: "rgba(192, 132, 252, 0.04)" },
    lineColor: { light: "#a855f7", dark: "#c084fc" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      9, 0.2,
      12, 0.4,
      14, 0.6,
    ],
    lineOpacity: 0.4,
  },
  {
    id: "neighborhoods",
    sourceLayer: "neighborhoods",
    minzoom: 9,
    fillColor: { light: "rgba(34, 197, 94, 0.05)", dark: "rgba(74, 222, 128, 0.06)" },
    lineColor: { light: "#22c55e", dark: "#4ade80" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      9, 0.5,
      12, 0.8,
      14, 1,
    ],
    lineOpacity: 0.5,
  },
] as const;

interface BoundaryLayersProps {
  uiTheme: "light" | "dark";
  layers: Record<string, boolean>;
}

/** Adds boundary polygon layers from the boundaries PMTiles source. */
export default function BoundaryLayers({ uiTheme, layers }: BoundaryLayersProps) {
  const { map, isLoaded } = useMap();

  // Add source + all boundary layers when map loads
  useEffect(() => {
    if (!isLoaded || !map) return;

    if (!map.getSource("boundaries")) {
      map.addSource("boundaries", {
        type: "vector",
        url: BOUNDARIES_PMTILES_URL,
      });
    }

    for (const def of BOUNDARY_LAYER_DEFS) {
      const visible = !!layers[def.id];

      // Fill layer (subtle semi-transparent)
      map.addLayer(
        {
          id: `${def.id}-fill`,
          type: "fill",
          source: "boundaries",
          "source-layer": def.sourceLayer,
          minzoom: def.minzoom,
          layout: {
            visibility: visible ? "visible" : "none",
          },
          paint: {
            "fill-color": def.fillColor[uiTheme],
            "fill-opacity": 1,
          },
        },
        LABEL_BEFORE_ID,
      );

      // Line layer (outlines)
      map.addLayer(
        {
          id: `${def.id}-line`,
          type: "line",
          source: "boundaries",
          "source-layer": def.sourceLayer,
          minzoom: def.minzoom,
          layout: {
            visibility: visible ? "visible" : "none",
          },
          paint: {
            "line-color": def.lineColor[uiTheme],
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            "line-width": def.lineWidth as any,
            "line-opacity": def.lineOpacity,
          },
        },
        LABEL_BEFORE_ID,
      );
    }

    return () => {
      try {
        for (const def of BOUNDARY_LAYER_DEFS) {
          if (map.getLayer(`${def.id}-line`)) map.removeLayer(`${def.id}-line`);
          if (map.getLayer(`${def.id}-fill`)) map.removeLayer(`${def.id}-fill`);
        }
        if (map.getSource("boundaries")) map.removeSource("boundaries");
      } catch {
        // Style may have been swapped
      }
    };
  }, [isLoaded, map, uiTheme]); // eslint-disable-line react-hooks/exhaustive-deps

  // Update layer visibility when toggles change
  useEffect(() => {
    if (!isLoaded || !map) return;

    for (const def of BOUNDARY_LAYER_DEFS) {
      const visible = !!layers[def.id];
      const visibility = visible ? "visible" : "none";

      try {
        if (map.getLayer(`${def.id}-fill`)) {
          map.setLayoutProperty(`${def.id}-fill`, "visibility", visibility);
        }
        if (map.getLayer(`${def.id}-line`)) {
          map.setLayoutProperty(`${def.id}-line`, "visibility", visibility);
        }
      } catch {
        // Layer may not exist yet
      }
    }
  }, [isLoaded, map, layers]);

  return null;
}

/** All boundary layer IDs (fill + line) for interaction queries. */
export const BOUNDARY_FILL_LAYER_IDS = BOUNDARY_LAYER_DEFS.map((d) => `${d.id}-fill`);
export const BOUNDARY_LINE_LAYER_IDS = BOUNDARY_LAYER_DEFS.map((d) => `${d.id}-line`);
