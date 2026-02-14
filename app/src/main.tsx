import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import maplibregl from "maplibre-gl";
import { Protocol } from "pmtiles";
import "./index.css";
import App from "./App.tsx";

// Register PMTiles protocol once at startup, before any <Map> mounts.
const protocol = new Protocol();
maplibregl.addProtocol("pmtiles", protocol.tile);

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
