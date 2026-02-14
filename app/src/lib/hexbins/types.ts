/** Types for the server-side H3 hexbin API. */

/** A single hexbin as returned by GET /api/hexbins (MessagePack decoded). */
export interface HexbinEntry {
  /** Hex boundary vertices as [[lng, lat], ...] (typically 6 points). */
  vertices: [number, number][];
  /** Filtered incident count in this hexagonal cell. */
  count: number;
}
