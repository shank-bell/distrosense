import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev the UI (5173) proxies the API and the WebSocket to the dashboard backend (8090).
// In production `npm run build` output is served by that backend itself, so no proxy is needed.
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": "http://localhost:8090",
      "/health": "http://localhost:8090",
      "/ws": { target: "ws://localhost:8090", ws: true },
    },
  },
});
