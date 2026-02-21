/** Types for the server-side sidebar API. */

/** A crime incident as returned by the sidebar API. */
export interface SidebarIncident {
  id: number;
  sourceId: number;
  sourceName: string;
  sourceIncidentId: string | null;
  subcategory: string;
  category: string;
  severity: number;
  longitude: number;
  latitude: number;
  occurredAt: string;
  description: string | null;
  blockAddress: string | null;
  city: string | null;
  state: string | null;
  arrestMade: boolean | null;
  locationType: string | null;
}

/** Response from GET /api/sidebar. */
export interface SidebarResponse {
  features: SidebarIncident[];
  totalCount: number;
  hasMore: boolean;
}

/** Bounding box as [west, south, east, north]. */
export type BBox = [number, number, number, number];
