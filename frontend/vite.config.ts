/// <reference types="vitest" />
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import path from "node:path";

// Vite config: build output goes into src/gibran/ui/static/ so the
// Python wheel (via Hatch's force-include) packs it. The dev server
// proxies /api/* to the local FastAPI on port 8000.
//
// Vitest reuses this config -- the `test` block adds the test-side
// concerns (happy-dom for browser globals, setup file for jest-dom
// matchers).
export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "src"),
    },
  },
  build: {
    outDir: path.resolve(__dirname, "../src/gibran/ui/static"),
    emptyOutDir: true,
    sourcemap: false,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: false,
      },
    },
  },
  test: {
    environment: "happy-dom",
    globals: true,
    setupFiles: ["./vitest.setup.ts"],
    css: false,  // skip CSS parsing; the styles aren't tested
  },
});
