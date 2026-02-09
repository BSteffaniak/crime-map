/**
 * React hook for the cluster web worker.
 *
 * Provides a singleton worker instance, debounced viewport updates,
 * filter synchronization, and sidebar pagination.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import type { FilterState } from "../types";
import type {
  BBox,
  SidebarFeature,
  WorkerRequest,
  WorkerResponse,
} from "./types";

interface ClusterWorkerState {
  clusters: GeoJSON.FeatureCollection;
  sidebarFeatures: SidebarFeature[];
  totalCount: number;
  loading: boolean;
  ready: boolean;
}

const EMPTY_FC: GeoJSON.FeatureCollection = {
  type: "FeatureCollection",
  features: [],
};

/** Debounce delay for viewport updates (ms). */
const VIEWPORT_DEBOUNCE = 150;

// Singleton worker shared across all hook instances
let sharedWorker: Worker | null = null;
let listenerCount = 0;
const listeners = new Set<(msg: WorkerResponse) => void>();

function getOrCreateWorker(): Worker {
  if (!sharedWorker) {
    sharedWorker = new Worker(
      new URL("./worker.ts", import.meta.url),
      { type: "module" },
    );
    sharedWorker.onmessage = (e: MessageEvent<WorkerResponse>) => {
      for (const listener of listeners) {
        listener(e.data);
      }
    };
    // Initialize with base URL (tiles served at /tiles/)
    send({ type: "init", baseUrl: "/tiles" });
  }
  return sharedWorker;
}

function send(msg: WorkerRequest) {
  getOrCreateWorker().postMessage(msg);
}

export function useClusterWorker(filters: FilterState) {
  const [state, setState] = useState<ClusterWorkerState>({
    clusters: EMPTY_FC,
    sidebarFeatures: [],
    totalCount: 0,
    loading: false,
    ready: false,
  });

  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastViewportRef = useRef<{ bbox: BBox; zoom: number } | null>(null);

  // Register message listener
  useEffect(() => {
    const handler = (msg: WorkerResponse) => {
      switch (msg.type) {
        case "ready":
          setState((prev) => ({ ...prev, ready: true }));
          break;
        case "viewport":
          setState((prev) => ({
            ...prev,
            clusters: msg.clusters,
            sidebarFeatures: msg.sidebarFeatures,
            totalCount: msg.totalCount,
            loading: false,
          }));
          break;
        case "moreSidebarFeatures":
          setState((prev) => ({
            ...prev,
            sidebarFeatures: [...prev.sidebarFeatures, ...msg.features],
          }));
          break;
        case "error":
          console.error("Cluster worker error:", msg.message);
          setState((prev) => ({ ...prev, loading: false }));
          break;
      }
    };

    listeners.add(handler);

    return () => {
      listeners.delete(handler);
      listenerCount--;
      if (listenerCount === 0 && sharedWorker) {
        sharedWorker.terminate();
        sharedWorker = null;
      }
    };
  }, []);

  // Sync filters to worker
  useEffect(() => {
    send({ type: "setFilters", filters });

    // Re-request current viewport with new filters
    if (lastViewportRef.current) {
      const { bbox, zoom } = lastViewportRef.current;
      setState((prev) => ({ ...prev, loading: true }));
      send({ type: "getViewport", bbox, zoom });
    }
  }, [filters]);

  // Debounced viewport update
  const updateViewport = useCallback((bbox: BBox, zoom: number) => {
    lastViewportRef.current = { bbox, zoom };

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      setState((prev) => ({ ...prev, loading: true }));
      send({ type: "getViewport", bbox, zoom });
    }, VIEWPORT_DEBOUNCE);
  }, []);

  // Get expansion zoom for a cluster
  const getExpansionZoom = useCallback((clusterId: number): Promise<number> => {
    return new Promise((resolve) => {
    getOrCreateWorker();
    listenerCount++;

    const handler = (msg: WorkerResponse) => {
        if (
          msg.type === "expansionZoom" &&
          msg.clusterId === clusterId
        ) {
          listeners.delete(handler);
          resolve(msg.zoom);
        }
      };
      listeners.add(handler);
      send({ type: "getExpansionZoom", clusterId });
    });
  }, []);

  // Load more sidebar features
  const loadMore = useCallback(() => {
    send({
      type: "getMoreSidebarFeatures",
      offset: state.sidebarFeatures.length,
      limit: 50,
    });
  }, [state.sidebarFeatures.length]);

  return {
    ...state,
    updateViewport,
    getExpansionZoom,
    loadMore,
  };
}
