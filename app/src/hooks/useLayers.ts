import { useCallback, useEffect, useState } from "react";
import { MAP_LAYERS, defaultLayerVisibility } from "@/lib/map-config";

const STORAGE_KEY = "mapLayers";
const URL_PARAM = "layers";

/** Reads layer visibility from localStorage. Returns null if not stored. */
function getStoredLayers(): Record<string, boolean> | null {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return null;
    const parsed = JSON.parse(stored) as Record<string, boolean>;
    // Validate that it's a plausible object
    if (typeof parsed !== "object" || parsed === null) return null;
    return parsed;
  } catch {
    return null;
  }
}

/** Saves layer visibility to localStorage. */
function storeLayers(layers: Record<string, boolean>): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(layers));
  } catch {
    // localStorage unavailable
  }
}

/** Parses layer visibility from URL search params. Returns null if no param. */
function parseLayersFromUrl(): Record<string, boolean> | null {
  const params = new URLSearchParams(window.location.search);
  const layersParam = params.get(URL_PARAM);
  if (!layersParam) return null;

  // URL format: layers=heatmap,hexbins,states (comma-separated visible layers)
  const visibleSet = new Set(layersParam.split(",").filter(Boolean));
  const result: Record<string, boolean> = {};
  for (const layer of MAP_LAYERS) {
    result[layer.id] = visibleSet.has(layer.id);
  }
  return result;
}

/** Serializes layer visibility to URL param value. Returns empty if all defaults. */
function serializeLayers(layers: Record<string, boolean>): string {
  const defaults = defaultLayerVisibility();
  const isDefault = MAP_LAYERS.every((l) => (layers[l.id] ?? l.defaultVisible) === defaults[l.id]);
  if (isDefault) return "";

  const visible = MAP_LAYERS.filter((l) => layers[l.id]).map((l) => l.id);
  return visible.join(",");
}

/**
 * Manages map layer visibility state with dual persistence:
 * - URL search params (takes priority on load)
 * - localStorage (fallback, always kept in sync)
 */
export function useLayers() {
  const [layers, setLayers] = useState<Record<string, boolean>>(() => {
    // Priority: URL params > localStorage > defaults
    const fromUrl = parseLayersFromUrl();
    if (fromUrl) return fromUrl;
    const fromStorage = getStoredLayers();
    if (fromStorage) {
      // Ensure any new layers from config get their defaults
      const merged = { ...defaultLayerVisibility(), ...fromStorage };
      return merged;
    }
    return defaultLayerVisibility();
  });

  // Sync to URL and localStorage on every change
  useEffect(() => {
    const serialized = serializeLayers(layers);
    const params = new URLSearchParams(window.location.search);

    if (serialized) {
      params.set(URL_PARAM, serialized);
    } else {
      params.delete(URL_PARAM);
    }

    const search = params.toString();
    const url = search ? `?${search}` : window.location.pathname;
    window.history.replaceState(null, "", url);

    storeLayers(layers);
  }, [layers]);

  const toggleLayer = useCallback((id: string) => {
    setLayers((prev) => ({ ...prev, [id]: !prev[id] }));
  }, []);

  const setLayerVisible = useCallback((id: string, visible: boolean) => {
    setLayers((prev) => ({ ...prev, [id]: visible }));
  }, []);

  const isLayerVisible = useCallback(
    (id: string): boolean => layers[id] ?? false,
    [layers],
  );

  return { layers, toggleLayer, setLayerVisible, isLayerVisible } as const;
}
