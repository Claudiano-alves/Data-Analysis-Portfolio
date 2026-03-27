import path from 'node:path'
import { fileURLToPath } from 'node:url'
import react from '@vitejs/plugin-react'
import sirv from 'sirv'
import { defineConfig } from 'vite'
import { viteStaticCopy } from 'vite-plugin-static-copy'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

/** Serve pastas estáticas na raiz do repo (dev): icons, img, logos_carteiras, data */
function serveRootStatic() {
  const iconsDir = path.resolve(__dirname, 'icons')
  const imgDir = path.resolve(__dirname, 'img')
  const logosDir = path.resolve(__dirname, 'logos_carteiras')
  const dataDir = path.resolve(__dirname, 'data')
  return {
    name: 'serve-root-static',
    configureServer(server) {
      server.middlewares.use(
        '/icons',
        sirv(iconsDir, { dev: true, etag: true })
      )
      server.middlewares.use('/img', sirv(imgDir, { dev: true, etag: true }))
      server.middlewares.use(
        '/logos_carteiras',
        sirv(logosDir, { dev: true, etag: true })
      )
      server.middlewares.use('/data', sirv(dataDir, { dev: true, etag: true }))
    },
  }
}

export default defineConfig({
  plugins: [
    react(),
    serveRootStatic(),
    viteStaticCopy({
      targets: [
        { src: 'icons/**', dest: 'icons' },
        { src: 'img/**', dest: 'img' },
        { src: 'logos_carteiras/**', dest: 'logos_carteiras' },
        { src: 'data/**', dest: 'data' },
      ],
    }),
  ],
  resolve: {
    alias: {
      '@icons': path.resolve(__dirname, 'icons'),
      '@img': path.resolve(__dirname, 'img'),
      '@logos_carteiras': path.resolve(__dirname, 'logos_carteiras'),
    },
  },
})
