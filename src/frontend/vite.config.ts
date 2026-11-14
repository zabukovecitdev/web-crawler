import { rmSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import react from '@vitejs/plugin-react'
import { loadEnv, type Plugin, type ProxyOptions } from 'vite'
import { defineConfig } from 'vitest/config'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

/** The MSW worker is copied from public/ into dist/; production never registers it, so drop it unless mocks are on. */
function dropMockWorker(mocksOn: boolean): Plugin {
  return {
    name: 'drop-mock-worker',
    apply: 'build',
    closeBundle() {
      if (!mocksOn) {
        rmSync(path.resolve(__dirname, 'dist/mockServiceWorker.js'), { force: true })
      }
    },
  }
}

/**
 * Local development without Google sign-in. When DEV_API_KEY is set, the dev server forwards /api and /hubs to
 * DEV_API_TARGET and adds the X-Api-Key header, so the API treats every call as the API key user.
 * The key stays in the dev server: it has no VITE_ prefix, so it never reaches the browser or the build.
 * Leave VITE_API_BASE empty so that the app calls the dev server.
 */
function devApiProxy(env: Record<string, string>): Record<string, ProxyOptions> | undefined {
  if (!env.DEV_API_KEY) {
    return undefined
  }
  const options: ProxyOptions = {
    target: env.DEV_API_TARGET || 'http://localhost:5080',
    changeOrigin: true,
    ws: true,
    headers: { 'X-Api-Key': env.DEV_API_KEY },
  }
  return { '/api': options, '/hubs': { ...options } }
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, __dirname, '')
  return {
    plugins: [react(), dropMockWorker(env.VITE_USE_MOCKS === 'true')],
    resolve: {
      alias: {
        '@': path.resolve(__dirname, './src'),
      },
    },
    server: {
      port: 5173,
      proxy: devApiProxy(env),
    },
    test: {
      environment: 'node',
      include: ['src/**/*.test.ts'],
    },
  }
})
