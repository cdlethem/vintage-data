import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";

export default defineConfig({
  plugins: [react()],
  build: {
    sourcemap: false,
    emptyOutDir: true,
    outDir: resolve(__dirname, "../src/airflow/providers/vintage/bot_dashboard/static/dashboard"),
    lib: { entry: resolve(__dirname, "src/dashboard.tsx"), name: "AirflowPlugin", formats: ["umd"] },
    rollupOptions: {
      external: ["react", "react-dom", "react-router-dom", "react/jsx-runtime", "@chakra-ui/react", "@emotion/react"],
      output: {
        entryFileNames: "main.[hash].umd.cjs",
        globals: { react: "React", "react-dom": "ReactDOM", "react-router-dom": "ReactRouterDOM", "react/jsx-runtime": "ReactJSXRuntime", "@chakra-ui/react": "ChakraUI", "@emotion/react": "EmotionReact" }
      }
    }
  },
  define: {
    global: "globalThis",
    "process.env": "{}",
    "process.env.NODE_ENV": JSON.stringify("production"),
  },
});
