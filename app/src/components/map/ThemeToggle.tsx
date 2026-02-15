import { useCallback, useEffect, useRef, useState } from "react";
import { Moon, Sun, Monitor, Contrast, Eclipse, ChevronDown, Check } from "lucide-react";
import { MAP_THEMES, type MapTheme } from "@/lib/map-config";

interface ThemeToggleProps {
  mapTheme: MapTheme;
  onSelect: (theme: MapTheme) => void;
}

const themeIcon: Record<MapTheme, React.ReactNode> = {
  light: <Sun className="h-4 w-4" />,
  dark: <Moon className="h-4 w-4" />,
  white: <Monitor className="h-4 w-4" />,
  grayscale: <Contrast className="h-4 w-4" />,
  black: <Eclipse className="h-4 w-4" />,
};

const themeLabel: Record<MapTheme, string> = {
  light: "Light",
  dark: "Dark",
  white: "White",
  grayscale: "Grayscale",
  black: "Black",
};

/** Floating theme selector dropdown positioned over the map. */
export default function ThemeToggle({ mapTheme, onSelect }: ThemeToggleProps) {
  const [open, setOpen] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const toggle = useCallback(() => setOpen((prev) => !prev), []);

  // Close on click outside
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setOpen(false);
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open]);

  return (
    <div ref={containerRef} className="relative">
      {/* Trigger button */}
      <button
        onClick={toggle}
        aria-label={`Theme: ${themeLabel[mapTheme]}`}
        aria-expanded={open}
        className="flex h-8 items-center gap-1.5 rounded-md border border-border bg-background px-2 shadow-sm transition-colors hover:bg-accent"
      >
        <span className="text-foreground">{themeIcon[mapTheme]}</span>
        <span className="text-xs font-medium text-foreground">{themeLabel[mapTheme]}</span>
        <ChevronDown className={`h-3 w-3 text-muted-foreground transition-transform ${open ? "rotate-180" : ""}`} />
      </button>

      {/* Dropdown panel */}
      {open && (
        <div className="mt-1 w-full min-w-[140px] overflow-hidden rounded-md border border-border bg-background shadow-lg">
          {MAP_THEMES.map((theme) => {
            const active = theme === mapTheme;
            return (
              <button
                key={theme}
                onClick={() => {
                  onSelect(theme);
                  setOpen(false);
                }}
                className={`flex w-full items-center gap-2 px-2.5 py-1.5 text-left text-xs transition-colors ${
                  active
                    ? "bg-accent font-medium text-foreground"
                    : "text-foreground hover:bg-accent/50"
                }`}
              >
                <span className="text-foreground">{themeIcon[theme]}</span>
                <span className="flex-1">{themeLabel[theme]}</span>
                {active && <Check className="h-3 w-3 text-foreground" />}
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}
