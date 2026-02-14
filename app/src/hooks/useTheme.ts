import { useCallback, useEffect, useState } from "react";
import {
  MAP_THEMES,
  baseUiTheme,
  defaultMapTheme,
  type MapTheme,
} from "@/lib/map-config";

type UiTheme = "light" | "dark";

const UI_STORAGE_KEY = "theme";
const MAP_STORAGE_KEY = "map-theme";

/** Reads the user's system color-scheme preference. */
function getSystemTheme(): UiTheme {
  if (typeof window === "undefined") return "light";
  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

/** Reads a manually-stored UI theme override from localStorage, if any. */
function getStoredUiTheme(): UiTheme | null {
  try {
    const stored = localStorage.getItem(UI_STORAGE_KEY);
    if (stored === "light" || stored === "dark") return stored;
  } catch {
    // localStorage unavailable (e.g. SSR)
  }
  return null;
}

/** Reads a manually-stored map theme override from localStorage, if any. */
function getStoredMapTheme(): MapTheme | null {
  try {
    const stored = localStorage.getItem(MAP_STORAGE_KEY);
    if (stored && (MAP_THEMES as readonly string[]).includes(stored)) {
      return stored as MapTheme;
    }
  } catch {
    // localStorage unavailable
  }
  return null;
}

/**
 * Theme hook with system preference detection, manual override, and
 * independent map theme cycling through all 5 Protomaps flavors.
 *
 * - `uiTheme`: "light" | "dark" — controls Tailwind classes, overlays
 * - `mapTheme`: one of 5 Protomaps flavors — controls the basemap style
 * - Defaults: system preference determines uiTheme; mapTheme defaults to
 *   matching uiTheme (light -> light, dark -> dark)
 * - Changing mapTheme also syncs uiTheme to the base UI theme of the
 *   selected flavor (e.g. selecting "black" sets uiTheme to "dark")
 */
export function useTheme() {
  const [uiTheme, setUiTheme] = useState<UiTheme>(
    () => getStoredUiTheme() ?? getSystemTheme(),
  );
  const [mapTheme, setMapThemeState] = useState<MapTheme>(
    () => getStoredMapTheme() ?? defaultMapTheme(getStoredUiTheme() ?? getSystemTheme()),
  );

  // Sync the .dark/.light class on <html> whenever uiTheme changes
  useEffect(() => {
    const root = document.documentElement;
    if (uiTheme === "dark") {
      root.classList.add("dark");
      root.classList.remove("light");
    } else {
      root.classList.add("light");
      root.classList.remove("dark");
    }
  }, [uiTheme]);

  // Listen for system preference changes (only applies when no manual override)
  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => {
      // Only auto-switch if user hasn't manually overridden
      if (!getStoredUiTheme()) {
        const newUi = e.matches ? "dark" : "light";
        setUiTheme(newUi);
        // Also reset map theme if the user hasn't manually overridden it
        if (!getStoredMapTheme()) {
          setMapThemeState(defaultMapTheme(newUi));
        }
      }
    };
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  /** Cycle through all 5 map themes in order. */
  const cycleMapTheme = useCallback(() => {
    setMapThemeState((prev) => {
      const idx = MAP_THEMES.indexOf(prev);
      const next = MAP_THEMES[(idx + 1) % MAP_THEMES.length];
      // Persist the manual choice
      try {
        localStorage.setItem(MAP_STORAGE_KEY, next);
      } catch {
        // ignore
      }
      // Sync the UI theme to match the new map theme
      const newUi = baseUiTheme(next);
      setUiTheme(newUi);
      try {
        localStorage.setItem(UI_STORAGE_KEY, newUi);
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  /** Set a specific map theme directly. */
  const setMapTheme = useCallback((theme: MapTheme) => {
    setMapThemeState(theme);
    try {
      localStorage.setItem(MAP_STORAGE_KEY, theme);
    } catch {
      // ignore
    }
    // Sync the UI theme to match
    const newUi = baseUiTheme(theme);
    setUiTheme(newUi);
    try {
      localStorage.setItem(UI_STORAGE_KEY, newUi);
    } catch {
      // ignore
    }
  }, []);

  const isDark = uiTheme === "dark";

  return { uiTheme, mapTheme, cycleMapTheme, setMapTheme, isDark } as const;
}
