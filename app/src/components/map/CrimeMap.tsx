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
import { severityColor } from "../../lib/types";

interface CrimeMapProps {
  onBoundsChange?: (bounds: maplibregl.LngLatBounds) => void;
}

export default function CrimeMap({ onBoundsChange }: CrimeMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [loaded, setLoaded] = useState(false);

  const setupLayers = useCallback((map: maplibregl.Map) => {
    // Add PMTiles source for crime incidents
    map.addSource("incidents", {
      type: "vector",
      url: "pmtiles:///tiles/incidents.pmtiles",
    });

    // Heatmap layer (visible at low zoom)
    map.addLayer({
      id: "incidents-heat",
      type: "heatmap",
      source: "incidents",
      "source-layer": "incidents",
      maxzoom: CLUSTER_MAX_ZOOM,
      paint: {
        "heatmap-weight": [
          "interpolate",
          ["linear"],
          ["get", "severity"],
          1,
          0.2,
          5,
          1,
        ],
        "heatmap-intensity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          0,
          0.5,
          HEATMAP_MAX_ZOOM,
          2,
        ],
        "heatmap-color": [
          "interpolate",
          ["linear"],
          ["heatmap-density"],
          0,
          "rgba(0, 0, 255, 0)",
          0.1,
          "rgba(65, 105, 225, 0.4)",
          0.3,
          "rgba(0, 200, 0, 0.5)",
          0.5,
          "rgba(255, 255, 0, 0.6)",
          0.7,
          "rgba(255, 165, 0, 0.8)",
          1,
          "rgba(255, 0, 0, 0.9)",
        ],
        "heatmap-radius": [
          "interpolate",
          ["linear"],
          ["zoom"],
          0,
          2,
          HEATMAP_MAX_ZOOM,
          20,
        ],
        "heatmap-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          HEATMAP_MAX_ZOOM - 1,
          0.8,
          CLUSTER_MAX_ZOOM,
          0,
        ],
      },
    });

    // Individual point layer (visible at higher zoom)
    map.addLayer({
      id: "incidents-points",
      type: "circle",
      source: "incidents",
      "source-layer": "incidents",
      minzoom: HEATMAP_MAX_ZOOM,
      paint: {
        "circle-radius": [
          "interpolate",
          ["linear"],
          ["zoom"],
          HEATMAP_MAX_ZOOM,
          2,
          14,
          6,
        ],
        "circle-color": [
          "match",
          ["get", "severity"],
          5,
          severityColor(5),
          4,
          severityColor(4),
          3,
          severityColor(3),
          2,
          severityColor(2),
          severityColor(1),
        ],
        "circle-stroke-width": 0.5,
        "circle-stroke-color": "#ffffff",
        "circle-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          HEATMAP_MAX_ZOOM,
          0.4,
          CLUSTER_MAX_ZOOM,
          0.85,
        ],
      },
    });

    // Popup on click
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
            <div class="text-gray-500 text-xs mt-1">${props.date ?? ""}</div>
            <div class="text-gray-500 text-xs">${props.city ?? ""}, ${props.state ?? ""}</div>
          </div>`,
        )
        .addTo(map);
    });

    map.on("mouseenter", "incidents-points", () => {
      map.getCanvas().style.cursor = "pointer";
    });
    map.on("mouseleave", "incidents-points", () => {
      map.getCanvas().style.cursor = "";
    });
  }, []);

  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    // Register PMTiles protocol
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
      onBoundsChange?.(map.getBounds());
    });

    map.on("moveend", () => {
      onBoundsChange?.(map.getBounds());
    });

    mapRef.current = map;

    return () => {
      maplibregl.removeProtocol("pmtiles");
      map.remove();
      mapRef.current = null;
    };
  }, [setupLayers, onBoundsChange]);

  return (
    <div ref={containerRef} className="h-full w-full">
      {!loaded && (
        <div className="flex h-full items-center justify-center bg-gray-100 text-gray-500">
          Loading map...
        </div>
      )}
    </div>
  );
}
