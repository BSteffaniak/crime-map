/** Crime category taxonomy mirroring the Rust backend. */

export const CRIME_CATEGORIES = {
  VIOLENT: {
    label: "Violent Crime",
    severity: 4,
    color: "#dc2626",
    subcategories: [
      { id: "HOMICIDE", label: "Homicide", severity: 5 },
      { id: "SEXUAL_ASSAULT", label: "Sexual Assault", severity: 5 },
      { id: "ROBBERY", label: "Robbery", severity: 4 },
      { id: "AGGRAVATED_ASSAULT", label: "Aggravated Assault", severity: 4 },
      { id: "SIMPLE_ASSAULT", label: "Simple Assault", severity: 3 },
    ],
  },
  PROPERTY: {
    label: "Property Crime",
    severity: 3,
    color: "#f59e0b",
    subcategories: [
      { id: "BURGLARY", label: "Burglary", severity: 3 },
      { id: "LARCENY_THEFT", label: "Larceny/Theft", severity: 2 },
      { id: "MOTOR_VEHICLE_THEFT", label: "Motor Vehicle Theft", severity: 3 },
      { id: "ARSON", label: "Arson", severity: 4 },
      { id: "VANDALISM", label: "Vandalism", severity: 2 },
    ],
  },
  DRUG_NARCOTICS: {
    label: "Drug/Narcotics",
    severity: 3,
    color: "#8b5cf6",
    subcategories: [
      { id: "DRUG_POSSESSION", label: "Drug Possession", severity: 2 },
      {
        id: "DRUG_SALES_MANUFACTURING",
        label: "Drug Sales/Manufacturing",
        severity: 3,
      },
      { id: "DRUG_EQUIPMENT", label: "Drug Equipment", severity: 1 },
    ],
  },
  PUBLIC_ORDER: {
    label: "Public Order",
    severity: 2,
    color: "#3b82f6",
    subcategories: [
      { id: "WEAPONS_VIOLATION", label: "Weapons Violation", severity: 3 },
      { id: "DUI", label: "DUI", severity: 2 },
      { id: "DISORDERLY_CONDUCT", label: "Disorderly Conduct", severity: 1 },
      { id: "TRESPASSING", label: "Trespassing", severity: 1 },
      { id: "PROSTITUTION", label: "Prostitution", severity: 2 },
    ],
  },
  FRAUD_FINANCIAL: {
    label: "Fraud/Financial",
    severity: 2,
    color: "#10b981",
    subcategories: [
      { id: "FRAUD", label: "Fraud", severity: 2 },
      { id: "FORGERY", label: "Forgery", severity: 2 },
      { id: "EMBEZZLEMENT", label: "Embezzlement", severity: 2 },
      { id: "IDENTITY_THEFT", label: "Identity Theft", severity: 3 },
    ],
  },
  OTHER: {
    label: "Other",
    severity: 1,
    color: "#6b7280",
    subcategories: [
      { id: "MISSING_PERSON", label: "Missing Person", severity: 1 },
      { id: "NON_CRIMINAL", label: "Non-Criminal", severity: 1 },
      { id: "UNKNOWN", label: "Unknown", severity: 1 },
    ],
  },
} as const;

export type CategoryId = keyof typeof CRIME_CATEGORIES;

export interface ApiIncident {
  id: number;
  category: string;
  subcategory: string;
  severity: string;
  severityValue: number;
  longitude: number;
  latitude: number;
  occurredAt: string;
  description: string | null;
  blockAddress: string | null;
  city: string;
  state: string;
  arrestMade: boolean | null;
  locationType: string | null;
}

export interface FilterState {
  categories: CategoryId[];
  subcategories: string[];
  severityMin: number;
  dateFrom: string | null;
  dateTo: string | null;
  datePreset: string | null;
  arrestMade: boolean | null;
  sources: string[];
  /** Selected state FIPS codes. */
  stateFips: string[];
  /** Selected county GEOIDs. */
  countyGeoids: string[];
  /** Selected place GEOIDs. */
  placeGeoids: string[];
  /** Selected tract GEOIDs. */
  tractGeoids: string[];
  /** Selected neighborhood IDs (e.g. "nbhd-42"). */
  neighborhoodIds: string[];
}

export const DEFAULT_FILTERS: FilterState = {
  categories: [],
  subcategories: [],
  severityMin: 1,
  dateFrom: null,
  dateTo: null,
  datePreset: null,
  arrestMade: null,
  sources: [],
  stateFips: [],
  countyGeoids: [],
  placeGeoids: [],
  tractGeoids: [],
  neighborhoodIds: [],
};

/** Severity color for the given 1-5 value. */
export function severityColor(value: number): string {
  switch (value) {
    case 5:
      return "#991b1b";
    case 4:
      return "#dc2626";
    case 3:
      return "#f59e0b";
    case 2:
      return "#3b82f6";
    default:
      return "#6b7280";
  }
}

/** Returns the color for a given category. */
export function categoryColor(category: string): string {
  const cat = CRIME_CATEGORIES[category as CategoryId];
  return cat?.color ?? "#6b7280";
}

/** A data source as returned by GET /api/sources. */
export interface ApiSource {
  id: string;
  name: string;
  recordCount: number;
  city: string;
  state: string;
  portalUrl: string | null;
}
