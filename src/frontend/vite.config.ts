import { rmSync } from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import react from '@vitejs/plugin-react'
import { loadEnv, type Plugin, type ProxyOptions } from 'vite'
import { defineConfig } from 'vitest/config'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

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
