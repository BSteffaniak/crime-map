import { useEffect, useRef, type ReactNode } from "react";

interface SidebarPanelProps {
  open: boolean;
  onClose: () => void;
  children: ReactNode;
}

/** Minimum mouse movement (px) to consider a gesture a drag rather than a click. */
const DRAG_THRESHOLD = 5;

/**
 * Floating sidebar panel that overlays the map.
 *
 * - Desktop (md+): floating panel with rounded corners, shadow, and margin
 *   from edges. Clicking outside (without dragging) or pressing Escape
 *   closes it. Scroll, wheel, and drag events pass through to the map.
 * - Mobile (<md): full-screen overlay with a semi-transparent backdrop.
 */
export default function SidebarPanel({ open, onClose, children }: SidebarPanelProps) {
  const panelRef = useRef<HTMLDivElement>(null);
  const mouseDownPos = useRef<{ x: number; y: number } | null>(null);

  // Close on Escape key
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handler);
    return () => document.removeEventListener("keydown", handler);
  }, [open, onClose]);

  // Close on click outside the panel (but not on drag/pan)
  useEffect(() => {
    if (!open) return;

    const onMouseDown = (e: MouseEvent) => {
      // Only track clicks that start outside the panel
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        mouseDownPos.current = { x: e.clientX, y: e.clientY };
      } else {
        mouseDownPos.current = null;
      }
    };

    const onMouseUp = (e: MouseEvent) => {
      if (!mouseDownPos.current) return;

      const dx = e.clientX - mouseDownPos.current.x;
      const dy = e.clientY - mouseDownPos.current.y;
      const distance = Math.sqrt(dx * dx + dy * dy);

      // Only close if the mouse barely moved (a click, not a drag/pan)
      if (distance < DRAG_THRESHOLD) {
        onClose();
      }

      mouseDownPos.current = null;
    };

    document.addEventListener("mousedown", onMouseDown);
    document.addEventListener("mouseup", onMouseUp);
    return () => {
      document.removeEventListener("mousedown", onMouseDown);
      document.removeEventListener("mouseup", onMouseUp);
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <>
      {/* Mobile backdrop — blocks map interaction on mobile only */}
      <div
        className="fixed inset-0 z-40 bg-black/40 md:hidden"
        onClick={onClose}
        aria-hidden
      />

      {/* Panel — fixed positioned, no full-screen wrapper blocking the map */}
      <div
        ref={panelRef}
        className={[
          "fixed z-40 flex flex-col bg-background text-foreground shadow-xl",
          // Mobile: full-screen
          "inset-0",
          // Desktop: floating panel with margin
          "md:inset-auto md:top-3 md:left-3 md:bottom-3 md:w-[380px] md:rounded-xl md:border md:border-border",
        ].join(" ")}
      >
        {children}
      </div>
    </>
  );
}
