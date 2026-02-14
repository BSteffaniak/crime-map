import { Moon, Sun } from "lucide-react";

interface ThemeToggleProps {
  isDark: boolean;
  onToggle: () => void;
}

/** Floating theme toggle button positioned over the map. */
export default function ThemeToggle({ isDark, onToggle }: ThemeToggleProps) {
  return (
    <button
      onClick={onToggle}
      aria-label={isDark ? "Switch to light mode" : "Switch to dark mode"}
      className="absolute top-2 left-2 z-10 flex h-8 w-8 items-center justify-center rounded-md border border-border bg-background shadow-sm transition-colors hover:bg-accent"
    >
      {isDark ? (
        <Sun className="h-4 w-4 text-foreground" />
      ) : (
        <Moon className="h-4 w-4 text-foreground" />
      )}
    </button>
  );
}
