import { useCallback, useEffect, useRef } from "react";
import { useMap } from "@/components/ui/map";
import { BOUNDARIES_PMTILES_URL, LABEL_BEFORE_ID } from "@/lib/map-config";
import type { AllBoundaryCounts, BoundaryMetric, BoundaryType } from "@/hooks/useBoundaryCounts";
import type { FilterState } from "@/lib/types";

/** Maps boundary type to the property name used as the feature ID in PMTiles. */
const PROMOTE_ID_MAP: Record<string, string> = {
  states: "fips",
  counties: "geoid",
  places: "geoid",
  tracts: "geoid",
  neighborhoods: "nbhd_id",
};

/** Maps boundary layer toggle ID to the API boundary type. */
const LAYER_TO_TYPE: Record<string, BoundaryType> = {
  states: "state",
  counties: "county",
  places: "place",
  tracts: "tract",
  neighborhoods: "neighborhood",
};

/** Maps API boundary type back to layer toggle ID. */
const TYPE_TO_LAYER: Record<string, string> = {
  state: "states",
  county: "counties",
  place: "places",
  tract: "tracts",
  neighborhood: "neighborhoods",
};

/** Boundary layer definitions with style properties. */
const BOUNDARY_LAYER_DEFS = [
  {
    id: "states",
    sourceLayer: "states",
    minzoom: 0,
    lineColor: { light: "#64748b", dark: "#94a3b8" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      0, 1,
      6, 1.5,
      10, 2,
    ],
    lineOpacity: 0.6,
    baseFillColor: { light: "rgba(100, 116, 139, 0.04)", dark: "rgba(148, 163, 184, 0.06)" },
  },
  {
    id: "counties",
    sourceLayer: "counties",
    minzoom: 4,
    lineColor: { light: "#94a3b8", dark: "#64748b" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      4, 0.4,
      8, 0.8,
      14, 1.2,
    ],
    lineOpacity: 0.5,
    baseFillColor: { light: "rgba(100, 116, 139, 0.03)", dark: "rgba(148, 163, 184, 0.04)" },
  },
  {
    id: "places",
    sourceLayer: "places",
    minzoom: 7,
    lineColor: { light: "#3b82f6", dark: "#60a5fa" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      7, 0.4,
      10, 0.8,
      14, 1,
    ],
    lineOpacity: 0.5,
    baseFillColor: { light: "rgba(59, 130, 246, 0.04)", dark: "rgba(96, 165, 250, 0.05)" },
  },
  {
    id: "tracts",
    sourceLayer: "tracts",
    minzoom: 9,
    lineColor: { light: "#a855f7", dark: "#c084fc" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      9, 0.2,
      12, 0.4,
      14, 0.6,
    ],
    lineOpacity: 0.4,
    baseFillColor: { light: "rgba(168, 85, 247, 0.03)", dark: "rgba(192, 132, 252, 0.04)" },
  },
  {
    id: "neighborhoods",
    sourceLayer: "neighborhoods",
    minzoom: 9,
    lineColor: { light: "#22c55e", dark: "#4ade80" },
    lineWidth: [
      "interpolate", ["linear"], ["zoom"],
      9, 0.5,
      12, 0.8,
      14, 1,
    ],
    lineOpacity: 0.5,
    baseFillColor: { light: "rgba(34, 197, 94, 0.05)", dark: "rgba(74, 222, 128, 0.06)" },
  },
] as const;

/** Color palette (yellow → orange → red). Alpha values kept low for readability. */
const CHOROPLETH_COLORS = [
  "rgba(255, 255, 178, 0.05)",
  "rgba(254, 217, 118, 0.12)",
  "rgba(254, 178, 76, 0.18)",
  "rgba(253, 141, 60, 0.24)",
  "rgba(240, 59, 32, 0.30)",
  "rgba(189, 0, 38, 0.35)",
];

/** Breakpoints for each metric — values at which the 6 color stops trigger. */
const CHOROPLETH_STOPS: Record<BoundaryMetric, number[]> = {
  count:      [0,  10,    50,   200,   500,  2000],
  per_capita: [0,   2,     8,    20,    50,   150],   // per 1K residents
  per_sq_mi:  [0,  20,   100,   500,  2000, 10000],
};

/** Builds the choropleth fill-color expression for a given metric. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function buildChoroplethExpr(metric: BoundaryMetric): any {
  const stops = CHOROPLETH_STOPS[metric];
  const interp: (string | number | string[])[] = [
    "interpolate", ["linear"], ["feature-state", "value"],
  ];
  for (let i = 0; i < stops.length; i++) {
    interp.push(stops[i], CHOROPLETH_COLORS[i]);
  }
  return [
    "case",
    ["!=", ["feature-state", "value"], null],
    interp,
    "rgba(0, 0, 0, 0)",
  ];
}

/** Maps API boundary type to its FilterState key. */
const TYPE_TO_FILTER_KEY: Record<string, keyof FilterState> = {
  state: "stateFips",
  county: "countyGeoids",
  place: "placeGeoids",
  tract: "tractGeoids",
  neighborhood: "neighborhoodIds",
};

interface BoundaryLayersProps {
  uiTheme: "light" | "dark";
  layers: Record<string, boolean>;
  allBoundaryCounts: AllBoundaryCounts;
  visibleBoundaryTypes: BoundaryType[];
  filters: FilterState;
  boundaryMetric: BoundaryMetric;
}

/** Adds boundary polygon layers from the boundaries PMTiles source. */
export default function BoundaryLayers({
  uiTheme,
  layers,
  allBoundaryCounts,
  visibleBoundaryTypes,
  filters,
  boundaryMetric,
}: BoundaryLayersProps) {
  const { map, isLoaded } = useMap();
  const prevFeatureStatesRef = useRef<{ sourceLayer: string; ids: string[] }[]>([]);
  const prevSelectedRef = useRef<{ sourceLayer: string; ids: string[] }[]>([]);

  // Add source + all boundary layers when map loads
  useEffect(() => {
    if (!isLoaded || !map) return;

    if (!map.getSource("boundaries")) {
      map.addSource("boundaries", {
        type: "vector",
        url: BOUNDARIES_PMTILES_URL,
        promoteId: PROMOTE_ID_MAP,
      });
    }

    for (const def of BOUNDARY_LAYER_DEFS) {
      const visible = !!layers[def.id];
      const boundaryType = LAYER_TO_TYPE[def.id];
      const hasChoropleth = visible && visibleBoundaryTypes.includes(boundaryType);

      // Fill layer — use choropleth expression if this layer is visible
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
            "fill-color": hasChoropleth
              ? buildChoroplethExpr(boundaryMetric)
              : def.baseFillColor[uiTheme],
            "fill-opacity": 1,
          },
        },
        LABEL_BEFORE_ID,
      );

      // Line layer (outlines) — thicker/highlighted when selected via feature-state
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
            "line-color": [
              "case",
              ["boolean", ["feature-state", "selected"], false],
              "#2563eb",
              def.lineColor[uiTheme],
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            ] as any,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            "line-width": def.lineWidth as any,
            "line-opacity": [
              "case",
              ["boolean", ["feature-state", "selected"], false],
              1,
              def.lineOpacity,
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            ] as any,
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

  // Update fill-color expression when visible boundary types or metric change
  useEffect(() => {
    if (!isLoaded || !map) return;

    for (const def of BOUNDARY_LAYER_DEFS) {
      const visible = !!layers[def.id];
      const boundaryType = LAYER_TO_TYPE[def.id];
      const hasChoropleth = visible && visibleBoundaryTypes.includes(boundaryType);

      try {
        if (map.getLayer(`${def.id}-fill`)) {
          map.setPaintProperty(
            `${def.id}-fill`,
            "fill-color",
            hasChoropleth ? buildChoroplethExpr(boundaryMetric) : def.baseFillColor[uiTheme],
          );
        }
      } catch {
        // Layer may not exist yet
      }
    }
  }, [isLoaded, map, visibleBoundaryTypes, layers, uiTheme, boundaryMetric]);

  // ── Choropleth feature-state application ──
  // Extracted into a callback so it can be called both from the useEffect
  // (when counts/metric change) and from a `sourcedata` listener (when
  // tiles finish loading after the counts already arrived).

  const applyFeatureStates = useCallback(() => {
    if (!isLoaded || !map || !map.getSource("boundaries")) return;

    // Clear previous feature states for all layers
    for (const prev of prevFeatureStatesRef.current) {
      for (const id of prev.ids) {
        try {
          map.setFeatureState(
            { source: "boundaries", sourceLayer: prev.sourceLayer, id },
            { value: null, count: null },
          );
        } catch {
          // Feature may not be in the current viewport
        }
      }
    }

    // No visible boundary layers — clear
    if (visibleBoundaryTypes.length === 0) {
      prevFeatureStatesRef.current = [];
      return;
    }

    // Build a lookup of feature properties (population, area) per source layer
    // by querying source features from loaded tiles.
    const getFeatureProps = (sourceLayer: string, idProp: string) => {
      const propMap = new Map<string, { population: number; area: number }>();
      try {
        const features = map.querySourceFeatures("boundaries", { sourceLayer });
        for (const f of features) {
          const fid = f.properties?.[idProp];
          if (fid == null) continue;
          const key = String(fid);
          if (propMap.has(key)) continue; // dedupe
          propMap.set(key, {
            population: Number(f.properties?.population) || 0,
            area: Number(f.properties?.land_area_sq_mi) || 0,
          });
        }
      } catch {
        // Source may not be ready
      }
      return propMap;
    };

    // Apply counts for all visible layers
    const newStates: { sourceLayer: string; ids: string[] }[] = [];

    for (const def of BOUNDARY_LAYER_DEFS) {
      const boundaryType = LAYER_TO_TYPE[def.id];
      if (!visibleBoundaryTypes.includes(boundaryType)) continue;

      const counts = allBoundaryCounts[boundaryType];
      if (!counts) continue;

      // Only query feature properties if we need them for normalization
      const needsProps = boundaryMetric !== "count";
      const idProp = PROMOTE_ID_MAP[def.id];
      const featureProps = needsProps ? getFeatureProps(def.sourceLayer, idProp) : null;

      const ids: string[] = [];
      for (const [geoid, rawCount] of Object.entries(counts)) {
        let displayValue = rawCount;

        if (boundaryMetric === "per_capita" && featureProps) {
          const props = featureProps.get(geoid);
          if (props && props.population > 0) {
            displayValue = (rawCount / props.population) * 1000; // per 1K residents
          } else {
            displayValue = 0; // No population data — hide
          }
        } else if (boundaryMetric === "per_sq_mi" && featureProps) {
          const props = featureProps.get(geoid);
          if (props && props.area > 0) {
            displayValue = rawCount / props.area;
          } else {
            displayValue = 0; // No area data — hide
          }
        }

        try {
          map.setFeatureState(
            { source: "boundaries", sourceLayer: def.sourceLayer, id: geoid },
            { value: displayValue, count: rawCount },
          );
          ids.push(geoid);
        } catch {
          // Feature not yet loaded in viewport
        }
      }
      if (ids.length > 0) {
        newStates.push({ sourceLayer: def.sourceLayer, ids });
      }
    }

    prevFeatureStatesRef.current = newStates;
  }, [isLoaded, map, allBoundaryCounts, visibleBoundaryTypes, boundaryMetric]);

  // Apply feature states when counts, metric, or visible layers change
  useEffect(() => {
    applyFeatureStates();
  }, [applyFeatureStates]);

  // Re-apply feature states when boundary tiles finish loading.
  // This handles the race condition where counts arrive before tiles are loaded,
  // so querySourceFeatures returns 0 features on the first pass.
  useEffect(() => {
    if (!isLoaded || !map) return;

    const onSourceData = (e: { sourceId: string; isSourceLoaded: boolean }) => {
      if (e.sourceId === "boundaries" && e.isSourceLoaded) {
        applyFeatureStates();
      }
    };

    map.on("sourcedata", onSourceData);
    return () => {
      map.off("sourcedata", onSourceData);
    };
  }, [isLoaded, map, applyFeatureStates]);

  // Apply "selected" feature state for filtered boundaries (highlight effect)
  useEffect(() => {
    if (!isLoaded || !map || !map.getSource("boundaries")) return;

    // Clear previous selected states
    for (const prev of prevSelectedRef.current) {
      for (const id of prev.ids) {
        try {
          map.setFeatureState(
            { source: "boundaries", sourceLayer: prev.sourceLayer, id },
            { selected: false },
          );
        } catch {
          // Feature may not be loaded
        }
      }
    }

    const newSelected: { sourceLayer: string; ids: string[] }[] = [];

    for (const def of BOUNDARY_LAYER_DEFS) {
      const boundaryType = LAYER_TO_TYPE[def.id];
      if (!boundaryType) continue;

      const filterKey = TYPE_TO_FILTER_KEY[boundaryType];
      if (!filterKey) continue;

      const selectedGeoids = filters[filterKey] as string[];
      if (selectedGeoids.length === 0) continue;

      const ids: string[] = [];
      for (const geoid of selectedGeoids) {
        try {
          map.setFeatureState(
            { source: "boundaries", sourceLayer: def.sourceLayer, id: geoid },
            { selected: true },
          );
          ids.push(geoid);
        } catch {
          // Feature may not be loaded in viewport
        }
      }
      if (ids.length > 0) {
        newSelected.push({ sourceLayer: def.sourceLayer, ids });
      }
    }

    prevSelectedRef.current = newSelected;
  }, [isLoaded, map, filters]);

  return null;
}

/** All boundary layer IDs (fill + line) for interaction queries. */
export const BOUNDARY_FILL_LAYER_IDS = BOUNDARY_LAYER_DEFS.map((d) => `${d.id}-fill`);
export const BOUNDARY_LINE_LAYER_IDS = BOUNDARY_LAYER_DEFS.map((d) => `${d.id}-line`);

/** Exported for use in click-to-filter. */
export { LAYER_TO_TYPE, TYPE_TO_LAYER };
