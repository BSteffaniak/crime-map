/**
 * React hooks for the sidebar web worker.
 *
 * Split into two hooks to avoid sequence number races:
 * - `useSidebarDriver(filters)` — used by CrimeMap. Sends all messages
 *   (setFilters, getSidebar) to the worker. Returns `updateSidebar`.
 * - `useSidebarReader()` — used by IncidentSidebar. Only reads responses
 *   from the worker. Returns sidebar data + loadMore.
 *
 * Both hooks share a singleton worker instance and listener set.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import type { FilterState } from "../types";
import type {
  BBox,
  SidebarFeature,
  WorkerRequest,
  WorkerResponse,
} from "./types";

/** Debounce delay for sidebar updates (ms). */
const SIDEBAR_DEBOUNCE = 150;

// -- Singleton worker --

let sharedWorker: Worker | null = null;
let refCount = 0;
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

function addRef() {
  getOrCreateWorker();
  refCount++;
}

function releaseRef() {
  refCount--;
  if (refCount === 0 && sharedWorker) {
    sharedWorker.terminate();
    sharedWorker = null;
  }
}

// -- Shared state (module-level, updated by listeners from both hooks) --

/** Global sequence counter. Only the driver increments this. */
let sidebarSeq = 0;

/** Shared sidebar state broadcast to all reader hooks via listeners. */
interface SharedSidebarState {
  sidebarFeatures: SidebarFeature[];
  totalCount: number;
  loading: boolean;
  dataProgress: {
    loaded: number;
    total: number | null;
    phase: "loading" | "indexing" | "complete" | "idle";
  };
}

let sharedState: SharedSidebarState = {
  sidebarFeatures: [],
  totalCount: 0,
  loading: false,
  dataProgress: { loaded: 0, total: null, phase: "idle" },
};

/** Callbacks for reader hooks to subscribe to state changes. */
const stateListeners = new Set<(state: SharedSidebarState) => void>();

function updateSharedState(updater: (prev: SharedSidebarState) => SharedSidebarState) {
  sharedState = updater(sharedState);
  for (const cb of stateListeners) {
    cb(sharedState);
  }
}

// Single worker response handler that updates shared state
const workerHandler = (msg: WorkerResponse) => {
  switch (msg.type) {
    case "progress":
      updateSharedState((prev) => ({
        ...prev,
        dataProgress: { loaded: msg.loaded, total: msg.total, phase: msg.phase },
      }));
      break;

    case "loadComplete":
      updateSharedState((prev) => ({
        ...prev,
        dataProgress: { loaded: msg.featureCount, total: msg.featureCount, phase: "complete" },
      }));
      break;

    case "sidebar":
      if (msg.seq === sidebarSeq) {
        updateSharedState((prev) => ({
          ...prev,
          sidebarFeatures: msg.sidebarFeatures,
          totalCount: msg.totalCount,
          loading: false,
        }));
      }
      break;

    case "moreSidebarFeatures":
      if (msg.seq === sidebarSeq) {
        updateSharedState((prev) => ({
          ...prev,
          sidebarFeatures: [...prev.sidebarFeatures, ...msg.features],
        }));
      }
      break;

    case "error":
      console.error("Sidebar worker error:", msg.message);
      updateSharedState((prev) => ({
        ...prev,
        loading: false,
        dataProgress: { ...prev.dataProgress, phase: "complete" },
      }));
      break;
  }
};

// Ensure the handler is registered
let handlerRegistered = false;
function ensureHandler() {
  if (!handlerRegistered) {
    listeners.add(workerHandler);
    handlerRegistered = true;
  }
}

// ============================================================
// Driver hook — used by CrimeMap only
// ============================================================

export function useSidebarDriver(filters: FilterState) {
  const debounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lastBboxRef = useRef<{ bbox: BBox; zoom: number } | null>(null);

  useEffect(() => {
    ensureHandler();
    addRef();
    return () => releaseRef();
  }, []);

  // Sync filters to worker + re-request sidebar
  useEffect(() => {
    send({ type: "setFilters", filters });

    // Re-request sidebar with updated filters
    if (lastBboxRef.current) {
      const { bbox, zoom } = lastBboxRef.current;
      const seq = ++sidebarSeq;
      updateSharedState((prev) => ({ ...prev, loading: true }));
      send({ type: "getSidebar", bbox, zoom, seq });
    }
  }, [filters]);

  const updateSidebar = useCallback((bbox: BBox, zoom: number) => {
    lastBboxRef.current = { bbox, zoom };

    if (debounceRef.current) {
      clearTimeout(debounceRef.current);
    }

    debounceRef.current = setTimeout(() => {
      const seq = ++sidebarSeq;
      updateSharedState((prev) => ({ ...prev, loading: true }));
      send({ type: "getSidebar", bbox, zoom, seq });
    }, SIDEBAR_DEBOUNCE);
  }, []);

  return { updateSidebar, dataProgress: sharedState.dataProgress };
}

// ============================================================
// Reader hook — used by IncidentSidebar only
// ============================================================

export function useSidebarReader() {
  const [state, setState] = useState<SharedSidebarState>(sharedState);

  useEffect(() => {
    ensureHandler();
    addRef();

    // Subscribe to shared state changes
    const cb = (newState: SharedSidebarState) => setState(newState);
    stateListeners.add(cb);

    // Sync current state immediately
    setState(sharedState);

    return () => {
      stateListeners.delete(cb);
      releaseRef();
    };
  }, []);

  const loadMore = useCallback(() => {
    send({
      type: "getMoreSidebarFeatures",
      offset: sharedState.sidebarFeatures.length,
      limit: 50,
      seq: sidebarSeq,
    });
  }, []);

  return {
    sidebarFeatures: state.sidebarFeatures,
    totalCount: state.totalCount,
    loading: state.loading,
    loadMore,
  };
}
