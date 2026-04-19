import { defineConfig } from "vite";
import preact from "@preact/preset-vite";

export default defineConfig({
  base: "/assets/",
  plugins: [preact()],
  root: "frontend",
  build: {
    outDir: "../src/webui_assets",
    emptyOutDir: true,
    assetsDir: "",
  },
});
