import { useCallback, useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import {
  MAP_STYLE,
  DEFAULT_CENTER,
  DEFAULT_ZOOM,
  HEATMAP_MAX_ZOOM,
  POINTS_MIN_ZOOM,
  HEX_COLOR_SCALE,
  HEX_MIN_COUNT,
  HEX_STROKE_OPACITY,
  hexFillOpacity,
} from "../../lib/map-config";
import { severityColor, type FilterState } from "../../lib/types";
import { buildIncidentFilter } from "../../lib/map-filters/expressions";
import type { HexbinEntry } from "../../lib/hexbins/types";

interface CrimeMapProps {
  filters: FilterState;
  hexbins: HexbinEntry[];
  zoom: number;
  onBoundsChange?: (bounds: maplibregl.LngLatBounds, zoom: number) => void;
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
 * Computes quantile-based step expressions for hex fill color based on
 * the current viewport's count distribution. Returns a MapLibre step
 * expression for fill-color.
 */
function buildHexColorSteps(hexbins: HexbinEntry[]): maplibregl.ExpressionSpecification {
  const visible = hexbins.filter((h) => h.count >= HEX_MIN_COUNT);
  if (visible.length === 0) {
    return ["step", ["get", "count"], HEX_COLOR_SCALE[0], 1, HEX_COLOR_SCALE[2]];
  }

  const counts = visible.map((h) => h.count).sort((a, b) => a - b);
  const quantile = (arr: number[], q: number) =>
    arr[Math.min(Math.floor(q * arr.length), arr.length - 1)];

  const p20 = quantile(counts, 0.2);
  const p40 = quantile(counts, 0.4);
  const p60 = quantile(counts, 0.6);
  const p80 = quantile(counts, 0.8);

  // If all counts are the same, use a single mid-range color
  if (p20 === p80) {
    return ["step", ["get", "count"], HEX_COLOR_SCALE[2], p80 + 1, HEX_COLOR_SCALE[2]];
  }

  // Build step expression with deduplicated thresholds
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const steps: any[] = ["step", ["get", "count"], HEX_COLOR_SCALE[0]];
  const breakpoints = [p20, p40, p60, p80];
  let lastThreshold = -Infinity;

  for (let i = 0; i < breakpoints.length; i++) {
    if (breakpoints[i] > lastThreshold) {
      steps.push(breakpoints[i], HEX_COLOR_SCALE[i + 1]);
      lastThreshold = breakpoints[i];
    }
  }

  return steps as maplibregl.ExpressionSpecification;
}

export default function CrimeMap({ filters, hexbins, zoom, onBoundsChange }: CrimeMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [loaded, setLoaded] = useState(false);
  const popupRef = useRef<maplibregl.Popup | null>(null);

  // -- Layer setup --

  const setupLayers = useCallback((map: maplibregl.Map) => {
    // PMTiles source for heatmap and individual points
    map.addSource("incidents", {
      type: "vector",
      url: "pmtiles:///tiles/incidents.pmtiles",
    });

    // GeoJSON source for H3 hexbin polygons
    map.addSource("hexbins", {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });

    // -- Layer 1: Heatmap (zoom 0+, persists underneath hexbins) --
    // No maxzoom — the heatmap stays visible underneath hexbins for context.
    // Opacity fades at higher zooms.
    map.addLayer({
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
        // Fade heatmap out as hexbins and points take over
        "heatmap-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          0, 0.8,
          HEATMAP_MAX_ZOOM - 1, 0.8,
          HEATMAP_MAX_ZOOM, 0.5,
          12, 0.25,
          14, 0.1,
          16, 0,
        ],
      },
    });

    // -- Layer 2: H3 hexbin fill (all zoom levels) --
    map.addLayer({
      id: "hexbin-fill",
      type: "fill",
      source: "hexbins",
      paint: {
        "fill-color": HEX_COLOR_SCALE[2],
        "fill-opacity": 0.5,
      },
    });

    // -- Layer 3: H3 hexbin outline (all zoom levels) --
    map.addLayer({
      id: "hexbin-outline",
      type: "line",
      source: "hexbins",
      paint: {
        "line-color": "#a50f15",
        "line-width": [
          "interpolate",
          ["linear"],
          ["zoom"],
          0, 0.3,
          8, 0.5,
          14, 1,
          18, 1.5,
        ],
        "line-opacity": HEX_STROKE_OPACITY,
      },
    });

    // -- Layer 4: Individual points from PMTiles (zoom POINTS_MIN_ZOOM+) --
    // Rendered on top of hex fill so individual dots show in sparse areas.
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
        "circle-stroke-color": "#ffffff",
        "circle-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          POINTS_MIN_ZOOM, 0.7,
          16, 0.9,
        ],
      },
    });

    // -- Click handlers --

    // Click hexbin to zoom in (expansion: zoom + 2)
    map.on("click", "hexbin-fill", (e) => {
      if (!e.features || e.features.length === 0) return;
      const coords = e.lngLat;
      const curZoom = map.getZoom();
      map.easeTo({ center: [coords.lng, coords.lat], zoom: curZoom + 2 });
    });

    // Click individual point for popup (PMTiles layer)
    // Skip if a hexbin covers this click point to avoid double-handling.
    map.on("click", "incidents-points", (e) => {
      const hexFeatures = map.queryRenderedFeatures(e.point, {
        layers: ["hexbin-fill"],
      });
      // Only skip if the hex has significant count (dense area)
      const denseHex = hexFeatures.find(
        (f) => f.properties && f.properties.count >= 10,
      );
      if (denseHex) return;

      const feature = e.features?.[0];
      if (!feature || !feature.properties) return;

      const props = feature.properties;
      const coords = (feature.geometry as GeoJSON.Point).coordinates.slice() as [
        number,
        number,
      ];

      // Close any existing popup
      popupRef.current?.remove();

      popupRef.current = new maplibregl.Popup({ offset: 10, maxWidth: "320px" })
        .setLngLat(coords)
        .setHTML(
          `<div class="text-sm">
            <div class="font-semibold">${props.subcategory ?? "Unknown"}</div>
            <div class="text-gray-600">${props.category ?? ""}</div>
            ${props.desc ? `<div class="text-gray-500 text-xs mt-1">${props.desc}</div>` : ""}
            <div class="text-gray-500 text-xs mt-1">${props.date ?? ""}</div>
            ${props.addr ? `<div class="text-gray-500 text-xs">${props.addr}</div>` : ""}
            <div class="text-gray-500 text-xs">${props.city ?? ""}, ${props.state ?? ""}</div>
          </div>`,
        )
        .addTo(map);
    });

    // Hover tooltip on hexbin
    const hoverPopup = new maplibregl.Popup({
      closeButton: false,
      closeOnClick: false,
      offset: 15,
      className: "hex-hover-popup",
    });

    map.on("mousemove", "hexbin-fill", (e) => {
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
          `<div class="text-xs font-medium px-1">${formatted} incident${count !== 1 ? "s" : ""}</div>`,
        )
        .addTo(map);
    });

    map.on("mouseleave", "hexbin-fill", () => {
      hoverPopup.remove();
    });

    // Cursor changes
    for (const layerId of ["hexbin-fill", "incidents-points"]) {
      map.on("mouseenter", layerId, () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", layerId, () => {
        map.getCanvas().style.cursor = "";
      });
    }
  }, []);

  // -- Map initialization --

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    const protocol = new Protocol();
    maplibregl.addProtocol("pmtiles", protocol.tile);

    const map = new maplibregl.Map({
      container: containerRef.current,
      style: MAP_STYLE,
      center: DEFAULT_CENTER,
      zoom: DEFAULT_ZOOM,
      attributionControl: {},
    });

    map.addControl(new maplibregl.NavigationControl(), "top-right");
    map.addControl(
      new maplibregl.GeolocateControl({
        positionOptions: { enableHighAccuracy: true },
        trackUserLocation: true,
      }),
      "top-right",
    );

    map.on("load", () => {
      setupLayers(map);
      setLoaded(true);

      const bounds = map.getBounds();
      const z = map.getZoom();
      onBoundsChange?.(bounds, z);
    });

    map.on("moveend", () => {
      const bounds = map.getBounds();
      const z = map.getZoom();
      onBoundsChange?.(bounds, z);
    });

    mapRef.current = map;

    return () => {
      maplibregl.removeProtocol("pmtiles");
      map.remove();
      mapRef.current = null;
    };
  }, [setupLayers, onBoundsChange]);

  // -- Update hexbin GeoJSON source when hexbins change --

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !loaded) return;

    const source = map.getSource("hexbins") as maplibregl.GeoJSONSource | undefined;
    if (!source) return;

    source.setData(hexbinsToGeoJSON(hexbins));

    // Update fill color based on quantile distribution
    const colorSteps = buildHexColorSteps(hexbins);
    map.setPaintProperty("hexbin-fill", "fill-color", colorSteps);
  }, [hexbins, loaded]);

  // -- Update hex fill opacity based on zoom --

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !loaded) return;

    map.setPaintProperty("hexbin-fill", "fill-opacity", hexFillOpacity(zoom));
  }, [zoom, loaded]);

  // -- Apply MapLibre filters on tile layers when filters change --

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !loaded) return;

    const filterExpr = buildIncidentFilter(filters);

    // Apply to heatmap and high-zoom individual points
    map.setFilter("incidents-heat", filterExpr);
    map.setFilter("incidents-points", filterExpr);
  }, [filters, loaded]);

  return (
    <div ref={containerRef} className="relative h-full w-full">
      {!loaded && (
        <div className="flex h-full items-center justify-center bg-gray-100 text-gray-500">
          Loading map...
        </div>
      )}
    </div>
  );
}
