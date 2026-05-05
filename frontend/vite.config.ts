import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), '');
  return {
    plugins: [react()],
    server: {
      // Listen on all network interfaces so the dev server is reachable via IP/LAN.
      host: true,
      // Allow accessing via any Host header (useful for LAN IP / custom domain mapping).
      // NOTE: This is vulnerable to DNS rebinding attacks; keep it for local/dev use only.
      allowedHosts: true,
      port: parseInt(env.VITE_PORT) || 5273,
      // Use dev-server proxy so frontend can call `/api/*` without CORS and without hardcoding LAN IP.
      proxy: {
        '/api': {
          target: env.VITE_API_PROXY_TARGET || 'http://localhost:8100',
          changeOrigin: true,
        },
      },
    },
    preview: {
      // Same for `vite preview`.
      host: true,
      allowedHosts: true,
      proxy: {
        '/api': {
          target: env.VITE_API_PROXY_TARGET || 'http://localhost:8100',
          changeOrigin: true,
        },
      },
    },
  }
})
