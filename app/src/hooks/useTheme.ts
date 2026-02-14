import { useCallback, useEffect, useState } from "react";

type Theme = "light" | "dark";

const STORAGE_KEY = "theme";

/** Reads the user's system color-scheme preference. */
function getSystemTheme(): Theme {
  if (typeof window === "undefined") return "light";
  return window.matchMedia("(prefers-color-scheme: dark)").matches
    ? "dark"
    : "light";
}

/** Reads a manually-stored override from localStorage, if any. */
function getStoredTheme(): Theme | null {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (stored === "light" || stored === "dark") return stored;
  } catch {
    // localStorage unavailable (e.g. SSR)
  }
  return null;
}

/**
 * Theme hook with system preference detection and manual override.
 *
 * - Defaults to system preference (`prefers-color-scheme`)
 * - Manual override saved to `localStorage("theme")`
 * - Toggles `.dark` class on `<html>` for Tailwind v4
 * - mapcn's `<Map>` auto-detects via MutationObserver on the class
 */
export function useTheme() {
  const [theme, setTheme] = useState<Theme>(
    () => getStoredTheme() ?? getSystemTheme(),
  );

  // Sync the .dark class on <html> whenever theme changes
  useEffect(() => {
    const root = document.documentElement;
    if (theme === "dark") {
      root.classList.add("dark");
      root.classList.remove("light");
    } else {
      root.classList.add("light");
      root.classList.remove("dark");
    }
  }, [theme]);

  // Listen for system preference changes (only applies when no manual override)
  useEffect(() => {
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const handler = (e: MediaQueryListEvent) => {
      // Only auto-switch if user hasn't manually overridden
      if (!getStoredTheme()) {
        setTheme(e.matches ? "dark" : "light");
      }
    };
    mq.addEventListener("change", handler);
    return () => mq.removeEventListener("change", handler);
  }, []);

  const toggleTheme = useCallback(() => {
    setTheme((prev) => {
      const next = prev === "dark" ? "light" : "dark";
      try {
        localStorage.setItem(STORAGE_KEY, next);
      } catch {
        // ignore
      }
      return next;
    });
  }, []);

  const isDark = theme === "dark";

  return { theme, toggleTheme, isDark } as const;
}
