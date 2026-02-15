import { useEffect, useRef, type ReactNode } from "react";

interface SidebarPanelProps {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
}

/**
 * Floating sidebar panel that overlays the map.
 *
 * - Desktop (md+): floating panel with rounded corners, shadow, and margin
 *   from edges. Clicking outside or pressing Escape closes it.
 * - Mobile (<md): full-screen overlay with a semi-transparent backdrop.
 */
export default function SidebarPanel({ open, onClose, children }: SidebarPanelProps) {
  const panelRef = useRef<HTMLDivElement>(null);

  // Close on Escape key
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open, onClose]);

  // Close on click outside the panel
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        onClose();
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-40">
      {/* Mobile backdrop */}
      <div className="absolute inset-0 bg-black/40 md:hidden" aria-hidden />

      {/* Panel */}
      <div
        ref={panelRef}
        className={[
          // Base
          "flex flex-col bg-background text-foreground shadow-xl",
          // Mobile: full-screen
          "absolute inset-0",
          // Desktop: floating panel with margin
          "md:inset-auto md:top-3 md:left-3 md:bottom-3 md:w-[380px] md:rounded-xl md:border md:border-border",
        ].join(" ")}
      >
        {children}
      </div>
    </div>
  );
}
