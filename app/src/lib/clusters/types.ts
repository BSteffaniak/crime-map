/** Types for the server-side cluster API. */

/** A single cluster as returned by GET /api/clusters. */
export interface ClusterEntry {
  /** Weighted centroid longitude. */
  lng: number;
  /** Weighted centroid latitude. */
  lat: number;
  /** Filtered incident count in this cluster. */
  count: number;
}
