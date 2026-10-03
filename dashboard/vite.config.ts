import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // Dev-only bridge to the real ForgeDB HTTP API, which has no CORS
    // middleware (see internal/api/server.go) and is not meant to grow
    // any for a diagnostics surface -- the browser instead talks to this
    // same-origin /api/* path, and Vite forwards it server-side where
    // CORS does not apply. Backend address is overridable via
    // FORGEDB_HTTP_ADDR for the (default-port) case where someone runs
    // the node on something other than :8080.
    proxy: {
      '/api': {
        target: process.env.FORGEDB_HTTP_ADDR ?? 'http://localhost:8080',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
