/**
 * React hook for the cluster web worker.
 *
 * Provides a singleton worker instance, debounced viewport updates with
 * sequence-number-based cancellation, filter synchronization, progress
 * tracking for the initial bulk load, and sidebar pagination.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import type { FilterState } from "../types";
import type {
  BBox,
  SidebarFeature,
  WorkerRequest,
  WorkerResponse,
} from "./types";

export interface ClusterWorkerState {
  clusters: GeoJSON.FeatureCollection;
  sidebarFeatures: SidebarFeature[];
  totalCount: number;
  loading: boolean;
  ready: boolean;
  /** Progress of the initial bulk data load. */
  dataProgress: {
    /** Number of points loaded so far. */
    loaded: number;
    /** Approximate total if known. */
    total: number | null;
    /** Current phase: loading from network, or indexing in memory. */
    phase: "loading" | "indexing" | "complete" | "idle";
  };
}

const EMPTY_FC: GeoJSON.FeatureCollection = {
  type: "FeatureCollection",
  features: [],
};

/** Debounce delay for viewport updates (ms). */
const VIEWPORT_DEBOUNCE = 100;

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
    // Initialize with base URL — triggers bulk FlatGeobuf load in the worker
    send({ type: "init", baseUrl: "/tiles" });
  }
  return sharedWorker;
}

function send(msg: WorkerRequest) {
  getOrCreateWorker().postMessage(msg);
}

/** Global sequence counter for viewport request cancellation. */
let viewportSeq = 0;

export function useClusterWorker(filters: FilterState) {
  const [state, setState] = useState<ClusterWorkerState>({
    clusters: EMPTY_FC,
    sidebarFeatures: [],
    totalCount: 0,
    loading: false,
    ready: false,
    dataProgress: { loaded: 0, total: null, phase: "idle" },
  });

  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastViewportRef = useRef<{ bbox: BBox; zoom: number } | null>(null);

  // Register message listener
  useEffect(() => {
    // Ensure the worker exists
    getOrCreateWorker();
    listenerCount++;

    const handler = (msg: WorkerResponse) => {
      switch (msg.type) {
        case "ready":
          setState((prev) => ({ ...prev, ready: true }));
          break;

        case "progress":
          setState((prev) => ({
            ...prev,
            dataProgress: {
              loaded: msg.loaded,
              total: msg.total,
              phase: msg.phase,
            },
          }));
          break;

        case "loadComplete":
          setState((prev) => ({
            ...prev,
            dataProgress: {
              loaded: msg.featureCount,
              total: msg.featureCount,
              phase: "complete",
            },
          }));
          // After data is loaded, request the current viewport if we have one
          if (lastViewportRef.current) {
            const { bbox, zoom } = lastViewportRef.current;
            const seq = ++viewportSeq;
            setState((prev) => ({ ...prev, loading: true }));
            send({ type: "getViewport", bbox, zoom, seq });
          }
          break;

        case "viewport":
          // Phase 4: Only accept if seq matches the latest request
          if (msg.seq === viewportSeq) {
            setState((prev) => ({
              ...prev,
              clusters: msg.clusters,
              sidebarFeatures: msg.sidebarFeatures,
              totalCount: msg.totalCount,
              loading: false,
            }));
          }
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
      const seq = ++viewportSeq;
      setState((prev) => ({ ...prev, loading: true }));
      send({ type: "getViewport", bbox, zoom, seq });
    }
  }, [filters]);

  // Debounced viewport update with sequence number
  const updateViewport = useCallback((bbox: BBox, zoom: number) => {
    lastViewportRef.current = { bbox, zoom };

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      const seq = ++viewportSeq;
      setState((prev) => ({ ...prev, loading: true }));
      send({ type: "getViewport", bbox, zoom, seq });
    }, VIEWPORT_DEBOUNCE);
  }, []);

  // Get expansion zoom for a cluster
  const getExpansionZoom = useCallback((clusterId: number): Promise<number> => {
    return new Promise((resolve) => {
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
