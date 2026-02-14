import { Moon, Sun, Monitor, Contrast, Eclipse } from "lucide-react";
import type { MapTheme } from "@/lib/map-config";

interface ThemeToggleProps {
  mapTheme: MapTheme;
  onCycle: () => void;
}

const themeIcon: Record<MapTheme, React.ReactNode> = {
  light: <Sun className="h-4 w-4 text-foreground" />,
  dark: <Moon className="h-4 w-4 text-foreground" />,
  white: <Monitor className="h-4 w-4 text-foreground" />,
  grayscale: <Contrast className="h-4 w-4 text-foreground" />,
  black: <Eclipse className="h-4 w-4 text-foreground" />,
};

const themeLabel: Record<MapTheme, string> = {
  light: "Light",
  dark: "Dark",
  white: "White",
  grayscale: "Grayscale",
  black: "Black",
};

/** Floating theme toggle button positioned over the map. Cycles through all Protomaps themes. */
export default function ThemeToggle({ mapTheme, onCycle }: ThemeToggleProps) {
  return (
    <button
      onClick={onCycle}
      aria-label={`Current theme: ${themeLabel[mapTheme]}. Click to switch.`}
      title={themeLabel[mapTheme]}
      className="absolute top-2 left-2 z-10 flex h-8 items-center gap-1.5 rounded-md border border-border bg-background px-2 shadow-sm transition-colors hover:bg-accent"
    >
      {themeIcon[mapTheme]}
      <span className="text-xs font-medium text-foreground">{themeLabel[mapTheme]}</span>
    </button>
  );
}
