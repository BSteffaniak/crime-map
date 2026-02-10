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
import { useClusterWorker } from "../../lib/cluster-worker";
import { buildIncidentFilter } from "../../lib/map-filters/expressions";
import type { BBox } from "../../lib/cluster-worker/types";

/** Formats a number with K/M suffixes for compact display. */
function formatCount(n: number): string {
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (n >= 1_000) return `${(n / 1_000).toFixed(0)}K`;
  return n.toLocaleString();
}

interface CrimeMapProps {
  filters: FilterState;
  onBoundsChange?: (bounds: maplibregl.LngLatBounds, zoom: number) => void;
}

export default function CrimeMap({ filters, onBoundsChange }: CrimeMapProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const mapRef = useRef<maplibregl.Map | null>(null);
  const [loaded, setLoaded] = useState(false);

  const {
    clusters,
    updateViewport,
    getExpansionZoom,
    dataProgress,
  } = useClusterWorker(filters);

  // -- Layer setup --

  const setupLayers = useCallback((map: maplibregl.Map) => {
    // PMTiles source for high-zoom individual points
    map.addSource("incidents", {
      type: "vector",
      url: "pmtiles:///tiles/incidents.pmtiles",
    });

    // GeoJSON source for Supercluster clusters (mid-zoom)
    map.addSource("incidents-clusters", {
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
        "heatmap-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          HEATMAP_MAX_ZOOM - 2, 0.8,
          HEATMAP_MAX_ZOOM, 0,
        ],
      },
    });

    // -- Layer 2: Cluster circles (zoom 8-11) --
    map.addLayer({
      id: "incidents-cluster-circles",
      type: "circle",
      source: "incidents-clusters",
      filter: ["has", "point_count"],
      minzoom: HEATMAP_MAX_ZOOM,
      maxzoom: CLUSTER_MAX_ZOOM,
      paint: {
        "circle-radius": [
          "step",
          ["get", "point_count"],
          15,   // < 10 points
          10, 20,
          50, 25,
          200, 30,
          1000, 40,
        ],
        "circle-color": [
          "step",
          ["get", "point_count"],
          "#51bbd6",
          10, "#f1f075",
          50, "#f28cb1",
          200, "#f59e0b",
          1000, "#dc2626",
        ],
        "circle-stroke-width": 2,
        "circle-stroke-color": "#ffffff",
        "circle-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          HEATMAP_MAX_ZOOM, 0,
          HEATMAP_MAX_ZOOM + 0.5, 0.85,
          CLUSTER_MAX_ZOOM - 0.5, 0.85,
          CLUSTER_MAX_ZOOM, 0,
        ],
      },
    });

    // Cluster count labels
    map.addLayer({
      id: "incidents-cluster-count",
      type: "symbol",
      source: "incidents-clusters",
      filter: ["has", "point_count"],
      minzoom: HEATMAP_MAX_ZOOM,
      maxzoom: CLUSTER_MAX_ZOOM,
      layout: {
        "text-field": "{point_count_abbreviated}",
        "text-font": ["Open Sans Regular"],
        "text-size": 12,
      },
      paint: {
        "text-color": "#333",
        "text-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          HEATMAP_MAX_ZOOM, 0,
          HEATMAP_MAX_ZOOM + 0.5, 1,
          CLUSTER_MAX_ZOOM - 0.5, 1,
          CLUSTER_MAX_ZOOM, 0,
        ],
      },
    });

    // Unclustered points from Supercluster (individual at cluster zoom)
    map.addLayer({
      id: "incidents-cluster-unclustered",
      type: "circle",
      source: "incidents-clusters",
      filter: ["!", ["has", "point_count"]],
      minzoom: HEATMAP_MAX_ZOOM,
      maxzoom: CLUSTER_MAX_ZOOM,
      paint: {
        "circle-radius": 4,
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
          HEATMAP_MAX_ZOOM, 0,
          HEATMAP_MAX_ZOOM + 0.5, 0.7,
          CLUSTER_MAX_ZOOM - 0.5, 0.7,
          CLUSTER_MAX_ZOOM, 0,
        ],
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
        "circle-opacity": [
          "interpolate",
          ["linear"],
          ["zoom"],
          CLUSTER_MAX_ZOOM, 0,
          CLUSTER_MAX_ZOOM + 0.5, 0.85,
        ],
      },
    });

    // -- Click handlers --

    // Click cluster to zoom in
    map.on("click", "incidents-cluster-circles", async (e) => {
      const feature = e.features?.[0];
      if (!feature || !feature.properties) return;

      const clusterId = feature.properties.cluster_id as number;
      const coords = (feature.geometry as GeoJSON.Point).coordinates as [number, number];

      const zoom = await getExpansionZoom(clusterId);
      map.easeTo({ center: coords, zoom: Math.min(zoom, CLUSTER_MAX_ZOOM) });
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
      "incidents-cluster-circles",
      "incidents-cluster-unclustered",
      "incidents-points",
    ]) {
      map.on("mouseenter", layerId, () => {
        map.getCanvas().style.cursor = "pointer";
      });
      map.on("mouseleave", layerId, () => {
        map.getCanvas().style.cursor = "";
      });
    }
  }, [getExpansionZoom]);

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

      // Trigger initial viewport update for the worker
      const bbox: BBox = [
        bounds.getWest(),
        bounds.getSouth(),
        bounds.getEast(),
        bounds.getNorth(),
      ];
      updateViewport(bbox, zoom);
    });

    map.on("moveend", () => {
      const bounds = map.getBounds();
      const zoom = map.getZoom();
      onBoundsChange?.(bounds, zoom);

      const bbox: BBox = [
        bounds.getWest(),
        bounds.getSouth(),
        bounds.getEast(),
        bounds.getNorth(),
      ];
      updateViewport(bbox, zoom);
    });

    mapRef.current = map;

    return () => {
      maplibregl.removeProtocol("pmtiles");
      map.remove();
      mapRef.current = null;
    };
  }, [setupLayers, onBoundsChange, updateViewport]);

  // -- Update cluster GeoJSON source when clusters change --

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !loaded) return;

    const source = map.getSource("incidents-clusters") as maplibregl.GeoJSONSource | undefined;
    if (source) {
      source.setData(clusters);
    }
  }, [clusters, loaded]);

  // -- Apply MapLibre filters on PMTiles layers when filters change --

  useEffect(() => {
    const map = mapRef.current;
    if (!map || !loaded) return;

    const filterExpr = buildIncidentFilter(filters);

    // Apply to both heatmap and points layers
    // null means "show all" — MapLibre accepts null to clear a filter
    map.setFilter("incidents-heat", filterExpr);
    map.setFilter("incidents-points", filterExpr);
  }, [filters, loaded]);

  const showDataLoading =
    loaded && dataProgress.phase !== "complete" && dataProgress.phase !== "idle";

  return (
    <div ref={containerRef} className="relative h-full w-full">
      {!loaded && (
        <div className="flex h-full items-center justify-center bg-gray-100 text-gray-500">
          Loading map...
        </div>
      )}

      {/* Phase 6: Data loading progress overlay */}
      {showDataLoading && (
        <div className="absolute bottom-6 left-1/2 z-10 -translate-x-1/2">
          <div className="rounded-lg bg-white/95 px-5 py-3 shadow-lg backdrop-blur-sm">
            <div className="flex items-center gap-3">
              <div className="h-4 w-4 animate-spin rounded-full border-2 border-blue-500 border-t-transparent" />
              <div className="text-sm">
                <span className="font-medium text-gray-800">
                  {dataProgress.phase === "indexing"
                    ? "Building spatial index..."
                    : `Loading incidents: ${formatCount(dataProgress.loaded)}`}
                </span>
              </div>
            </div>
            {dataProgress.loaded > 0 && dataProgress.phase === "loading" && (
              <div className="mt-2 h-1.5 w-48 overflow-hidden rounded-full bg-gray-200">
                <div
                  className="h-full rounded-full bg-blue-500 transition-all duration-300"
                  style={{
                    width: dataProgress.total
                      ? `${Math.min(100, (dataProgress.loaded / dataProgress.total) * 100)}%`
                      : "60%",
                  }}
                />
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}
