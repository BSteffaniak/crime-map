import { useEffect, useState } from "react";
import type { ApiSource } from "@/lib/types";

/** Fetches the list of data sources from GET /api/sources. */
export function useSources(): ApiSource[] {
  const [sources, setSources] = useState<ApiSource[]>([]);

  useEffect(() => {
    fetch("/api/sources")
      .then((res) => {
        if (!res.ok) throw new Error(`Sources API ${res.status}`);
        return res.json() as Promise<ApiSource[]>;
      })
      .then(setSources)
      .catch((err) => console.error("Failed to fetch sources:", err));
  }, []);

  return sources;
}
