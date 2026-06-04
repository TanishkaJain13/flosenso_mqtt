import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// In dev, proxy /api to the FastAPI backend so the frontend can use same-origin
// relative URLs (no CORS juggling). Override the target with VITE_API_TARGET.
const API_TARGET = process.env.VITE_API_TARGET || "http://3.7.113.12:8500/";

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: API_TARGET,
        changeOrigin: true,
      },
    },
  },
});
