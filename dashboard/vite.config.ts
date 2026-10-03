import react from '@vitejs/plugin-react'
import { defineConfig, loadEnv } from 'vite'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  // Third argument '' (not the default 'VITE_') also loads FORGEDB_API_TOKEN
  // and FORGEDB_HTTP_ADDR from a local dashboard/.env -- safe here because
  // loadEnv's result is only read inside this Node-side config function,
  // never passed to `define`/`import.meta.env`, so neither var reaches the
  // browser bundle (see forgedbApi.ts's API_BASE_PATH comment: only
  // VITE_-prefixed vars are ever inlined into client JS).
  const env = loadEnv(mode, process.cwd(), '')

  return {
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
          target: env.FORGEDB_HTTP_ADDR ?? 'http://localhost:8080',
          changeOrigin: true,
          rewrite: (path) => path.replace(/^\/api/, ''),
          // internal/api/server.go's authMiddleware requires a Bearer
          // token on every endpoint except /health and /ready (see
          // internal/api/auth.go), and forgedbApi.ts deliberately never
          // attaches one from browser code -- doing that would bake a
          // real FORGEDB_API_TOKEN into the built JS bundle, readable by
          // anyone who opens the deployed dashboard (see this file's
          // "dashboard/.env.example" and deploy/README.md's Section 1.1).
          // Without this, /cluster, /kv/*, and /metrics would 401 even in
          // local dev, leaving the dashboard's "live" mode able to reach
          // only /health. This proxy itself runs server-side in Node, so
          // injecting the header here (when the operator sets
          // FORGEDB_API_TOKEN in a local, untracked dashboard/.env or
          // shell env -- never a committed file) never exposes the token
          // to the browser; when it's unset, behavior is identical to
          // before this fix.
          configure: (proxy) => {
            const token = env.FORGEDB_API_TOKEN
            if (!token) return
            proxy.on('proxyReq', (proxyReq) => {
              proxyReq.setHeader('Authorization', `Bearer ${token}`)
            })
          },
        },
      },
    },
  }
})
