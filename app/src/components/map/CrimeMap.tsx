import { useCallback, useEffect, useMemo, useRef } from "react";
import maplibregl from "maplibre-gl";
import { Map, useMap, MapControls } from "@/components/ui/map";
import {
  DEFAULT_CENTER,
  DEFAULT_ZOOM,
  LABEL_BEFORE_ID,
  HEATMAP_MAX_ZOOM,
  POINTS_MIN_ZOOM,
  HEX_MIN_COUNT,
  HEX_STROKE_OPACITY,
  HEX_OPACITY_RANGE,
  VIEWPORT_THROTTLE_MS,
  hexFillColor,
  hexFillOpacity,
  hexOutlineColor,
  hexbinResolution,
  pointStrokeColor,
  buildProtomapsStyle,
  baseUiTheme,
  type MapTheme,
} from "@/lib/map-config";
import { severityColor, type FilterState } from "@/lib/types";
import { buildIncidentFilter } from "@/lib/map-filters/expressions";
import type { HexbinEntry } from "@/lib/hexbins/types";

interface CrimeMapProps {
  filters: FilterState;
  hexbins: HexbinEntry[];
  zoom: number;
  mapTheme: MapTheme;
  onBoundsChange?: (bounds: maplibregl.LngLatBounds, zoom: number, options: { settled: boolean }) => void;
}

/** Converts HexbinEntry[] to a GeoJSON FeatureCollection of Polygons. */
function hexbinsToGeoJSON(hexbins: HexbinEntry[]): GeoJSON.FeatureCollection {
  return {
    type: "FeatureCollection",
    features: hexbins
      .filter((h) => h.count >= HEX_MIN_COUNT)
      .map((h) => ({
        type: "Feature" as const,
        geometry: {
          type: "Polygon" as const,
          // Close the ring by repeating the first vertex
          coordinates: [
            [...h.vertices.map(([lng, lat]) => [lng, lat]), [h.vertices[0][0], h.vertices[0][1]]],
          ],
        },
        properties: { count: h.count },
      })),
  };
}

/**
 * Computes a quantile-based interpolate expression that maps each feature's
 * `count` property to an opacity value. The base [min, max] range from config
 * is scaled by the zoom-dependent envelope so hexes naturally fade at extreme
 * zoom levels.
 */
function buildHexOpacityExpr(
  hexbins: HexbinEntry[],
  zoomScale: number,
): maplibregl.ExpressionSpecification {
  const [baseMin, baseMax] = HEX_OPACITY_RANGE;
  const oMin = baseMin * zoomScale;
  const oMax = baseMax * zoomScale;

  const visible = hexbins.filter((h) => h.count >= HEX_MIN_COUNT);
  if (visible.length === 0) {
    return ["literal", oMin];
  }

  const counts = visible.map((h) => h.count).sort((a, b) => a - b);
  const lo = counts[0];
  const hi = counts[counts.length - 1];

  // If all counts identical, use a mid-range opacity
  if (lo === hi) {
    return ["literal", (oMin + oMax) / 2];
  }

  const quantile = (q: number) =>
    counts[Math.min(Math.floor(q * counts.length), counts.length - 1)];

  const p25 = quantile(0.25);
  const p50 = quantile(0.50);
  const p75 = quantile(0.75);

  // Lerp helper: map a quantile position (0-1) to the opacity range
  const lerp = (t: number) => oMin + t * (oMax - oMin);

  // Build interpolation with deduplicated breakpoints
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const expr: any[] = ["interpolate", ["linear"], ["get", "count"]];
  const stops: [number, number][] = [
    [lo, lerp(0)],
    [p25, lerp(0.25)],
    [p50, lerp(0.50)],
    [p75, lerp(0.75)],
    [hi, lerp(1)],
  ];

  let lastCount = -Infinity;
  for (const [count, opacity] of stops) {
    if (count > lastCount) {
      expr.push(count, opacity);
      lastCount = count;
    }
  }

  // Need at least 2 stops for interpolate
  if (expr.length < 7) {
    return ["literal", (oMin + oMax) / 2];
  }

  return expr as maplibregl.ExpressionSpecification;
}

/**
 * Builds an outline opacity expression that scales with feature count.
 * Range: 0.1 (low count) to HEX_STROKE_OPACITY (high count).
 */
function buildOutlineOpacityExpr(
  hexbins: HexbinEntry[],
): maplibregl.ExpressionSpecification {
  const visible = hexbins.filter((h) => h.count >= HEX_MIN_COUNT);
  if (visible.length === 0) {
    return ["literal", 0.1];
  }

  const counts = visible.map((h) => h.count).sort((a, b) => a - b);
  const lo = counts[0];
  const hi = counts[counts.length - 1];

  if (lo === hi) {
    return ["literal", (0.1 + HEX_STROKE_OPACITY) / 2];
  }

  return [
    "interpolate",
    ["linear"],
    ["get", "count"],
    lo, 0.1,
    hi, HEX_STROKE_OPACITY,
  ] as maplibregl.ExpressionSpecification;
}

// ---------------------------------------------------------------------------
// Child layer components that use useMap() to add layers to the map.
// When the theme changes, mapcn swaps the style which resets isLoaded,
// then child useEffects re-fire and re-add layers on the new style.
// ---------------------------------------------------------------------------

/** Heatmap layer from PMTiles vector source. */
function HeatmapLayer({
  filters,
  uiTheme,
}: {
  filters: FilterState;
  uiTheme: "light" | "dark";
}) {
  const { map, isLoaded } = useMap();

  useEffect(() => {
    if (!isLoaded || !map) return;

    if (!map.getSource("incidents")) {
      map.addSource("incidents", {
        type: "vector",
        url: "pmtiles:///tiles/incidents.pmtiles",
      });
    }

    map.addLayer(
      {
        id: "incidents-heat",
        type: "heatmap",
        source: "incidents",
        "source-layer": "incidents",
        paint: {
          "heatmap-weight": [
            "interpolate",
            ["linear"],
            ["get", "severity"],
            1, 0.2,
            5, 1,
          ],
          "heatmap-intensity": [
            "interpolate",
            ["linear"],
            ["zoom"],
            0, 0.5,
            HEATMAP_MAX_ZOOM, 2,
            14, 3,
          ],
          "heatmap-color": [
            "interpolate",
            ["linear"],
            ["heatmap-density"],
            0, "rgba(0, 0, 255, 0)",
            0.1, "rgba(65, 105, 225, 0.4)",
            0.3, "rgba(0, 200, 0, 0.5)",
            0.5, "rgba(255, 255, 0, 0.6)",
            0.7, "rgba(255, 165, 0, 0.8)",
            1, "rgba(255, 0, 0, 0.9)",
          ],
          "heatmap-radius": [
            "interpolate",
            ["linear"],
            ["zoom"],
            0, 2,
            HEATMAP_MAX_ZOOM, 20,
            14, 30,
          ],
          "heatmap-opacity": [
            "interpolate",
            ["linear"],
            ["zoom"],
            0, 0.8,
            HEATMAP_MAX_ZOOM - 1, 0.8,
            HEATMAP_MAX_ZOOM, 0.5,
            9, 0.3,
            10, 0.1,
            11, 0.08,
            13, 0.06,
            15, 0.04,
            18, 0.02,
          ],
        },
      },
      LABEL_BEFORE_ID,
    );

    // Apply any active filters
    const filterExpr = buildIncidentFilter(filters);
    map.setFilter("incidents-heat", filterExpr);

    return () => {
      try {
        if (map.getLayer("incidents-heat")) map.removeLayer("incidents-heat");
      } catch {
        // Style may have been swapped
      }
    };
  }, [isLoaded, map, uiTheme, filters]);

  return null;
}

/** H3 hexbin fill + outline layers (GeoJSON source). */
function HexbinLayer({
  hexbins,
  zoom,
  uiTheme,
}: {
  hexbins: HexbinEntry[];
  zoom: number;
  uiTheme: "light" | "dark";
}) {
  const { map, isLoaded } = useMap();
  const sourceAddedRef = useRef(false);

  // Add source + layers when map style is loaded
  useEffect(() => {
    if (!isLoaded || !map) return;

    map.addSource("hexbins", {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });
    sourceAddedRef.current = true;

    // Single fill color; density represented via per-feature opacity
    map.addLayer(
      {
        id: "hexbin-fill",
        type: "fill",
        source: "hexbins",
        paint: {
          "fill-color": hexFillColor(uiTheme),
          "fill-opacity": 0,
        },
      },
      LABEL_BEFORE_ID,
    );

    map.addLayer(
      {
        id: "hexbin-outline",
        type: "line",
        source: "hexbins",
        paint: {
          "line-color": hexOutlineColor(uiTheme),
          "line-width": [
            "interpolate",
            ["linear"],
            ["zoom"],
            0, 0.3,
            8, 0.5,
            14, 1,
            18, 1.5,
          ],
          "line-opacity": 0.1,
        },
      },
      LABEL_BEFORE_ID,
    );

    return () => {
      try {
        if (map.getLayer("hexbin-outline")) map.removeLayer("hexbin-outline");
        if (map.getLayer("hexbin-fill")) map.removeLayer("hexbin-fill");
        if (map.getSource("hexbins")) map.removeSource("hexbins");
      } catch {
        // Style may have been swapped
      }
      sourceAddedRef.current = false;
    };
  }, [isLoaded, map, uiTheme]);

  // Update hexbin GeoJSON data + opacity expressions when hexbins or zoom change
  useEffect(() => {
    if (!isLoaded || !map || !sourceAddedRef.current) return;

    const source = map.getSource("hexbins") as maplibregl.GeoJSONSource | undefined;
    if (!source) return;

    source.setData(hexbinsToGeoJSON(hexbins));

    // Per-feature fill opacity scaled by zoom envelope
    const zoomScale = hexFillOpacity(zoom);
    const fillOpacityExpr = buildHexOpacityExpr(hexbins, zoomScale);
    map.setPaintProperty("hexbin-fill", "fill-opacity", fillOpacityExpr);

    // Per-feature outline opacity (not zoom-scaled, just count-based)
    const outlineOpacityExpr = buildOutlineOpacityExpr(hexbins);
    map.setPaintProperty("hexbin-outline", "line-opacity", outlineOpacityExpr);
  }, [hexbins, zoom, isLoaded, map]);

  return null;
}

/** Individual incident points from PMTiles (appears at high zoom). */
function IncidentPointsLayer({
  filters,
  uiTheme,
}: {
  filters: FilterState;
  uiTheme: "light" | "dark";
}) {
  const { map, isLoaded } = useMap();

  useEffect(() => {
    if (!isLoaded || !map) return;

    // Ensure the incidents source exists (HeatmapLayer may have added it)
    if (!map.getSource("incidents")) {
      map.addSource("incidents", {
        type: "vector",
        url: "pmtiles:///tiles/incidents.pmtiles",
      });
    }

    // Points go on top of everything (no beforeId)
    map.addLayer({
      id: "incidents-points",
      type: "circle",
      source: "incidents",
      "source-layer": "incidents",
      minzoom: POINTS_MIN_ZOOM,
      paint: {
        "circle-radius": [
          "interpolate",
          ["linear"],
          ["zoom"],
          POINTS_MIN_ZOOM, 3,
          16, 6,
          18, 10,
        ],
        "circle-color": [
          "match",
          ["get", "severity"],
          5, severityColor(5),
          4, severityColor(4),
          3, severityColor(3),
          2, severityColor(2),
          severityColor(1),
        ],
        "circle-stroke-width": 0.5,
        "circle-stroke-color": pointStrokeColor(uiTheme),
        "circle-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          POINTS_MIN_ZOOM, 0.7,
          16, 0.9,
        ],
      },
    });

    // Apply any active filters
    const filterExpr = buildIncidentFilter(filters);
    map.setFilter("incidents-points", filterExpr);

    return () => {
      try {
        if (map.getLayer("incidents-points")) map.removeLayer("incidents-points");
      } catch {
        // Style may have been swapped
      }
    };
  }, [isLoaded, map, uiTheme, filters]);

  return null;
}

/** Click + hover interactions for hexbin and incident point layers. */
function MapInteractions() {
  const { map, isLoaded } = useMap();
  const popupRef = useRef<maplibregl.Popup | null>(null);
  const hoverPopupRef = useRef<maplibregl.Popup | null>(null);

  useEffect(() => {
    if (!isLoaded || !map) return;

    // Click hexbin to zoom in (expansion: zoom + 2)
    const onHexClick = (e: maplibregl.MapLayerMouseEvent) => {
      if (!e.features || e.features.length === 0) return;

      // Don't zoom if the click also hit an incident point — let the
      // point popup handler take priority.
      if (map.getLayer("incidents-points")) {
        const pointFeatures = map.queryRenderedFeatures(e.point, {
          layers: ["incidents-points"],
        });
        if (pointFeatures.length > 0) return;
      }

      const curZoom = map.getZoom();

      // Don't zoom at high zoom — finest H3 resolution is already active
      // and individual incident dots are visible.
      if (curZoom >= POINTS_MIN_ZOOM + 1) return;

      const coords = e.lngLat;
      map.easeTo({ center: [coords.lng, coords.lat], zoom: curZoom + 2 });
    };

    // Click individual point for popup
    const onPointClick = (e: maplibregl.MapLayerMouseEvent) => {
      const feature = e.features?.[0];
      if (!feature || !feature.properties) return;

      const props = feature.properties;
      const coords = (feature.geometry as GeoJSON.Point).coordinates.slice() as [
        number,
        number,
      ];

      popupRef.current?.remove();

      popupRef.current = new maplibregl.Popup({ offset: 10, maxWidth: "320px" })
        .setLngLat(coords)
        .setHTML(
          `<div class="text-sm bg-white dark:bg-gray-900 rounded-lg shadow-lg p-3 border border-border">
            <div class="font-semibold">${props.subcategory ?? "Unknown"}</div>
            <div class="text-gray-600 dark:text-gray-400">${props.category ?? ""}</div>
            ${props.desc ? `<div class="text-gray-500 dark:text-gray-400 text-xs mt-1">${props.desc}</div>` : ""}
            <div class="text-gray-500 dark:text-gray-400 text-xs mt-1">${props.date ?? ""}</div>
            ${props.addr ? `<div class="text-gray-500 dark:text-gray-400 text-xs">${props.addr}</div>` : ""}
            <div class="text-gray-500 dark:text-gray-400 text-xs">${props.city ?? ""}, ${props.state ?? ""}</div>
          </div>`,
        )
        .addTo(map);
    };

    // Hover tooltip on hexbin
    const hoverPopup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      offset: 15,
      className: "hex-hover-popup",
    });
    hoverPopupRef.current = hoverPopup;

    const onHexMouseMove = (e: maplibregl.MapLayerMouseEvent) => {
      if (!e.features || e.features.length === 0) {
        hoverPopup.remove();
        return;
      }

      const props = e.features[0].properties;
      if (!props) return;

      const count = props.count as number;
      const formatted =
        count >= 1_000_000
          ? `${(count / 1_000_000).toFixed(1)}M`
          : count >= 1_000
            ? `${(count / 1_000).toFixed(1)}K`
            : String(count);

      hoverPopup
        .setLngLat(e.lngLat)
        .setHTML(
          `<div class="text-xs font-medium px-2 py-1 bg-white dark:bg-gray-900 rounded-md shadow-md border border-border">${formatted} incident${count !== 1 ? "s" : ""}</div>`,
        )
        .addTo(map);
    };

    const onHexMouseLeave = () => {
      hoverPopup.remove();
    };

    // Cursor changes
    const onLayerEnter = () => {
      map.getCanvas().style.cursor = "pointer";
    };
    const onLayerLeave = () => {
      map.getCanvas().style.cursor = "";
    };

    // Bind events (guard with getLayer to avoid errors if layer not yet added)
    if (map.getLayer("hexbin-fill")) {
      map.on("click", "hexbin-fill", onHexClick);
      map.on("mousemove", "hexbin-fill", onHexMouseMove);
      map.on("mouseleave", "hexbin-fill", onHexMouseLeave);
      map.on("mouseenter", "hexbin-fill", onLayerEnter);
      map.on("mouseleave", "hexbin-fill", onLayerLeave);
    }
    if (map.getLayer("incidents-points")) {
      map.on("click", "incidents-points", onPointClick);
      map.on("mouseenter", "incidents-points", onLayerEnter);
      map.on("mouseleave", "incidents-points", onLayerLeave);
    }

    return () => {
      try {
        map.off("click", "hexbin-fill", onHexClick);
        map.off("mousemove", "hexbin-fill", onHexMouseMove);
        map.off("mouseleave", "hexbin-fill", onHexMouseLeave);
        map.off("mouseenter", "hexbin-fill", onLayerEnter);
        map.off("mouseleave", "hexbin-fill", onLayerLeave);
        map.off("click", "incidents-points", onPointClick);
        map.off("mouseenter", "incidents-points", onLayerEnter);
        map.off("mouseleave", "incidents-points", onLayerLeave);
      } catch {
        // ignore
      }
      popupRef.current?.remove();
      hoverPopupRef.current?.remove();
    };
  }, [isLoaded, map]);

  return null;
}

/**
 * Emits viewport updates for bbox/zoom tracking with smart throttling.
 *
 * - `moveend`: always fires immediately with `settled: true`
 * - `move` (continuous): throttled to VIEWPORT_THROTTLE_MS intervals with
 *   `settled: false`. If the H3 resolution changes (zoom crosses a bracket
 *   boundary), fires immediately and resets the throttle timer.
 */
function BoundsTracker({
  onBoundsChange,
}: {
  onBoundsChange: (bounds: maplibregl.LngLatBounds, zoom: number, options: { settled: boolean }) => void;
}) {
  const { map, isLoaded } = useMap();
  const onBoundsChangeRef = useRef(onBoundsChange);
  onBoundsChangeRef.current = onBoundsChange;

  useEffect(() => {
    if (!isLoaded || !map) return;

    let lastFireTime = 0;
    let lastResolution = hexbinResolution(map.getZoom());
    let throttleTimer: ReturnType<typeof setTimeout> | null = null;

    const fire = (settled: boolean) => {
      lastFireTime = Date.now();
      lastResolution = hexbinResolution(map.getZoom());
      onBoundsChangeRef.current(map.getBounds(), map.getZoom(), { settled });
    };

    // Fire initial bounds (settled — this is the starting position)
    fire(true);

    // Continuous move: throttled, with instant bypass on resolution change
    const onMove = () => {
      const currentResolution = hexbinResolution(map.getZoom());
      const resolutionChanged = currentResolution !== lastResolution;
      const elapsed = Date.now() - lastFireTime;

      if (resolutionChanged) {
        // Resolution changed — fire immediately and reset throttle
        if (throttleTimer) {
          clearTimeout(throttleTimer);
          throttleTimer = null;
        }
        fire(false);
        return;
      }

      // Same resolution — throttle to VIEWPORT_THROTTLE_MS
      if (elapsed >= VIEWPORT_THROTTLE_MS) {
        fire(false);
      } else if (!throttleTimer) {
        // Schedule a trailing fire for the remaining time
        throttleTimer = setTimeout(() => {
          throttleTimer = null;
          fire(false);
        }, VIEWPORT_THROTTLE_MS - elapsed);
      }
    };

    // moveend: always fire immediately (settled)
    const onMoveEnd = () => {
      if (throttleTimer) {
        clearTimeout(throttleTimer);
        throttleTimer = null;
      }
      fire(true);
    };

    map.on("move", onMove);
    map.on("moveend", onMoveEnd);

    return () => {
      if (throttleTimer) {
        clearTimeout(throttleTimer);
      }
      map.off("move", onMove);
      map.off("moveend", onMoveEnd);
    };
  }, [isLoaded, map]);

  return null;
}

// ---------------------------------------------------------------------------
// Main CrimeMap component
// ---------------------------------------------------------------------------

export default function CrimeMap({ filters, hexbins, zoom, mapTheme, onBoundsChange }: CrimeMapProps) {
  const uiTheme = baseUiTheme(mapTheme);
  // Memoize the style so the Map component gets a stable reference per theme
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const style = useMemo(() => buildProtomapsStyle(mapTheme), [mapTheme]);

  const handleBoundsChange = useCallback(
    (bounds: maplibregl.LngLatBounds, z: number, options: { settled: boolean }) => {
      onBoundsChange?.(bounds, z, options);
    },
    [onBoundsChange],
  );

  return (
    <Map
      style={style}
      center={DEFAULT_CENTER}
      zoom={DEFAULT_ZOOM}
      className="h-full w-full"
    >
      <MapControls
        position="top-right"
        showZoom
        showLocate
        showCompass
      />
      <HeatmapLayer filters={filters} uiTheme={uiTheme} />
      <HexbinLayer hexbins={hexbins} zoom={zoom} uiTheme={uiTheme} />
      <IncidentPointsLayer filters={filters} uiTheme={uiTheme} />
      <MapInteractions />
      <BoundsTracker onBoundsChange={handleBoundsChange} />
    </Map>
  );
}
