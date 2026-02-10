/**
 * React hook for the sidebar web worker.
 *
 * Provides a singleton worker instance for loading sidebar incident data
 * from FlatGeobuf at zoom 8-11. At zoom 12+, sidebar data comes from
 * queryRenderedFeatures() in the map component instead.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import type { FilterState } from "../types";
import type {
  BBox,
  SidebarFeature,
  WorkerRequest,
  WorkerResponse,
} from "./types";

export interface SidebarWorkerState {
  sidebarFeatures: SidebarFeature[];
  totalCount: number;
  loading: boolean;
  ready: boolean;
  /** Progress of the current viewport FlatGeobuf load. */
  dataProgress: {
    loaded: number;
    total: number | null;
    phase: "loading" | "indexing" | "complete" | "idle";
  };
}

/** Debounce delay for sidebar updates (ms). */
const SIDEBAR_DEBOUNCE = 150;

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
    send({ type: "init", baseUrl: "/tiles" });
  }
  return sharedWorker;
}

function send(msg: WorkerRequest) {
  getOrCreateWorker().postMessage(msg);
}

/** Global sequence counter for sidebar request cancellation. */
let sidebarSeq = 0;

export function useSidebarWorker(filters: FilterState) {
  const [state, setState] = useState<SidebarWorkerState>({
    sidebarFeatures: [],
    totalCount: 0,
    loading: false,
    ready: false,
    dataProgress: { loaded: 0, total: null, phase: "idle" },
  });

  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastRequestRef = useRef<{ bbox: BBox; zoom: number } | null>(null);

  // Register message listener
  useEffect(() => {
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
          break;

        case "sidebar":
          if (msg.seq === sidebarSeq) {
            setState((prev) => ({
              ...prev,
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
          console.error("Sidebar worker error:", msg.message);
          setState((prev) => ({
            ...prev,
            loading: false,
            dataProgress: { ...prev.dataProgress, phase: "complete" },
          }));
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

    // Re-request sidebar with new filters
    if (lastRequestRef.current) {
      const { bbox, zoom } = lastRequestRef.current;
      const seq = ++sidebarSeq;
      setState((prev) => ({ ...prev, loading: true }));
      send({ type: "getSidebar", bbox, zoom, seq });
    }
  }, [filters]);

  // Debounced sidebar update
  const updateSidebar = useCallback((bbox: BBox, zoom: number) => {
    lastRequestRef.current = { bbox, zoom };

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      const seq = ++sidebarSeq;
      setState((prev) => ({ ...prev, loading: true }));
      send({ type: "getSidebar", bbox, zoom, seq });
    }, SIDEBAR_DEBOUNCE);
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
    updateSidebar,
    loadMore,
  };
}
