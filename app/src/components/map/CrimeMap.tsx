import { useCallback, useEffect, useRef, useState } from "react";
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import {
  MAP_STYLE,
  DEFAULT_CENTER,
  DEFAULT_ZOOM,
  HEATMAP_MAX_ZOOM,
  CLUSTER_MAX_ZOOM,
} from "../../lib/map-config";
import { severityColor, type FilterState } from "../../lib/types";
import { buildIncidentFilter } from "../../lib/map-filters/expressions";
import type { ClusterEntry } from "../../lib/clusters/types";

interface CrimeMapProps {
  filters: FilterState;
  clusters: ClusterEntry[];
  onBoundsChange?: (bounds: maplibregl.LngLatBounds, zoom: number) => void;
}

export default function CrimeMap({ filters, clusters, onBoundsChange }: CrimeMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [loaded, setLoaded] = useState(false);

  // -- Layer setup --

  const setupLayers = useCallback((map: maplibregl.Map) => {
    // PMTiles source for heatmap and individual points
    map.addSource("incidents", {
      type: "vector",
      url: "pmtiles:///tiles/incidents.pmtiles",
    });

    // GeoJSON source for server-provided clusters
    map.addSource("server-clusters", {
      type: "geojson",
      data: { type: "FeatureCollection", features: [] },
    });

    // -- Layer 1: Heatmap (zoom 0-7) --
    map.addLayer({
      id: "incidents-heat",
      type: "heatmap",
      source: "incidents",
      "source-layer": "incidents",
      maxzoom: HEATMAP_MAX_ZOOM,
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
        ],
        "heatmap-opacity": 0.8,
      },
    });

    // -- Layer 2a: Server-side cluster circles (zoom 8-11) --
    map.addLayer({
      id: "server-cluster-circles",
      type: "circle",
      source: "server-clusters",
      minzoom: HEATMAP_MAX_ZOOM,
      maxzoom: CLUSTER_MAX_ZOOM,
      paint: {
        "circle-radius": [
          "step",
          ["get", "count"],
          8,
          10, 11,
          50, 15,
          200, 20,
          1000, 28,
        ],
        "circle-color": [
          "step",
          ["get", "count"],
          "#51bbd6",
          10, "#f1f075",
          50, "#f28cb1",
          200, "#f59e0b",
          1000, "#dc2626",
        ],
        "circle-stroke-width": 1.5,
        "circle-stroke-color": "#ffffff",
        "circle-opacity": 0.85,
      },
    });

    // -- Layer 2b: Server-side cluster count labels --
    map.addLayer({
      id: "server-cluster-count",
      type: "symbol",
      source: "server-clusters",
      minzoom: HEATMAP_MAX_ZOOM,
      maxzoom: CLUSTER_MAX_ZOOM,
      layout: {
        "text-field": [
          "case",
          [">=", ["get", "count"], 1_000_000],
          ["concat", ["to-string", ["/", ["round", ["/", ["get", "count"], 100_000]], 10]], "M"],
          [">=", ["get", "count"], 10_000],
          ["concat", ["to-string", ["round", ["/", ["get", "count"], 1000]]], "K"],
          [">=", ["get", "count"], 1_000],
          ["concat", ["to-string", ["/", ["round", ["/", ["get", "count"], 100]], 10]], "K"],
          ["to-string", ["get", "count"]],
        ],
        "text-font": ["Open Sans Regular"],
        "text-size": 12,
      },
      paint: {
        "text-color": "#333",
        "text-opacity": 1,
      },
    });

    // -- Layer 3: Individual points from PMTiles (zoom 12+) --
    map.addLayer({
      id: "incidents-points",
      type: "circle",
      source: "incidents",
      "source-layer": "incidents",
      minzoom: CLUSTER_MAX_ZOOM,
      paint: {
        "circle-radius": [
          "interpolate",
          ["linear"],
          ["zoom"],
          CLUSTER_MAX_ZOOM, 3,
          16, 8,
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
        "circle-opacity": 0.85,
      },
    });

    // -- Click handlers --

    // Click cluster to zoom in
    map.on("click", "server-cluster-circles", (e) => {
      const feature = e.features?.[0];
      if (!feature) return;

      const coords = (feature.geometry as GeoJSON.Point).coordinates as [number, number];
      const curZoom = map.getZoom();
      map.easeTo({ center: coords, zoom: Math.min(curZoom + 2, CLUSTER_MAX_ZOOM) });
    });

    // Click individual point for popup (PMTiles layer)
    map.on("click", "incidents-points", (e) => {
      const feature = e.features?.[0];
      if (!feature || !feature.properties) return;

      const props = feature.properties;
      const coords = (feature.geometry as GeoJSON.Point).coordinates.slice() as [
        number,
        number,
      ];

      new maplibregl.Popup({ offset: 10, maxWidth: "320px" })
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

    // Cursor changes
    for (const layerId of [
      "server-cluster-circles",
      "incidents-points",
    ]) {
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
      const zoom = map.getZoom();
      onBoundsChange?.(bounds, zoom);
    });

    map.on("moveend", () => {
      const bounds = map.getBounds();
      const zoom = map.getZoom();
      onBoundsChange?.(bounds, zoom);
    });

    mapRef.current = map;

    return () => {
      maplibregl.removeProtocol("pmtiles");
      map.remove();
      mapRef.current = null;
    };
  }, [setupLayers, onBoundsChange]);

  // -- Update server-cluster GeoJSON source when clusters change --

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !loaded) return;

    const source = map.getSource("server-clusters") as maplibregl.GeoJSONSource | undefined;
    if (!source) return;

    source.setData({
      type: "FeatureCollection",
      features: clusters.map((c) => ({
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [c.lng, c.lat] },
        properties: { count: c.count },
      })),
    });
  }, [clusters, loaded]);

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
