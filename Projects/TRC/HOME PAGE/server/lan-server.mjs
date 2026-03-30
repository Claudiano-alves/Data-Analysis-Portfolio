/**
 * Servidor HTTP para a rede local (escuta em 0.0.0.0).
 *
 * Uso:
 *   npm run build
 *   npm run start:lan
 *
 * Porta: variável de ambiente PORT (predefinida 8080). Para http://mistrc.com
 * sem :porta no browser, use PORT=80 (PowerShell como administrador):
 *   $env:PORT=80; node server/lan-server.mjs
 *
 * Nome mistrc.com: em cada PC cliente edite o ficheiro hosts — ver server/HOSTS-mistrc.txt
 *
 * Credenciais Windows (ex.: .\usuariotrc) não são guardadas aqui; inicie a consola
 * com o utilizador que preferir. PORT 80 em Windows costuma exigir elevacao.
 */
import fs from 'node:fs'
import http from 'node:http'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import {
  createReportsHandlers,
  createUsersHandlers,
} from './api-handlers.mjs'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const rootDir = path.resolve(__dirname, '..')
const distDir = path.join(rootDir, 'dist')

const PORT = Number(process.env.PORT) || 8080
const HOST = process.env.HOST || '0.0.0.0'

const reportsApi = createReportsHandlers(rootDir)
const usersApi = createUsersHandlers(rootDir)

function safePathWithin(base, target) {
  const normalized = path.normalize(target)
  const baseNorm = path.normalize(base)
  if (!normalized.startsWith(baseNorm)) return null
  return normalized
}

function sendFile(res, filePath) {
  if (!fs.existsSync(filePath) || !fs.statSync(filePath).isFile()) {
    res.writeHead(404)
    res.end('Not found')
    return
  }
  const ext = path.extname(filePath).toLowerCase()
  const types = {
    '.html': 'text/html; charset=utf-8',
    '.js': 'application/javascript; charset=utf-8',
    '.css': 'text/css; charset=utf-8',
    '.json': 'application/json; charset=utf-8',
    '.png': 'image/png',
    '.jpg': 'image/jpeg',
    '.jpeg': 'image/jpeg',
    '.webp': 'image/webp',
    '.svg': 'image/svg+xml',
    '.ico': 'image/x-icon',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
  }
  const type = types[ext] || 'application/octet-stream'
  const buf = fs.readFileSync(filePath)
  res.writeHead(200, {
    'Content-Type': type,
    'Content-Length': buf.length,
    'Cache-Control': ext === '.html' ? 'no-cache' : 'public, max-age=3600',
  })
  res.end(buf)
}

function tryStaticFromDir(res, dirRoot, urlPath) {
  const rel = urlPath.replace(/^\/+/, '') || 'index.html'
  const full = safePathWithin(dirRoot, path.join(dirRoot, rel))
  if (!full) return false
  if (fs.existsSync(full) && fs.statSync(full).isFile()) {
    sendFile(res, full)
    return true
  }
  return false
}

async function onRequest(req, res) {
  const url = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`)
  const pathname = url.pathname

  try {
    if (pathname === '/api/reports' || pathname.startsWith('/api/reports/')) {
      if (req.method === 'POST') return await reportsApi.handlePost(req, res)
      if (req.method === 'DELETE') return await reportsApi.handleDelete(req, res)
      res.writeHead(405)
      return res.end('Method Not Allowed')
    }

    if (pathname === '/api/users' || pathname.startsWith('/api/users/')) {
      if (req.method === 'POST') return await usersApi.handlePost(req, res)
      if (req.method === 'PATCH') return await usersApi.handlePatch(req, res)
      if (req.method === 'DELETE') return await usersApi.handleDelete(req, res)
      res.writeHead(405)
      return res.end('Method Not Allowed')
    }

    const projectMounts = [
      ['/data', path.join(rootDir, 'data')],
      ['/img', path.join(rootDir, 'img')],
      ['/icons', path.join(rootDir, 'icons')],
      ['/logos_carteiras', path.join(rootDir, 'logos_carteiras')],
    ]

    for (const [prefix, dir] of projectMounts) {
      if (pathname === prefix || pathname.startsWith(`${prefix}/`)) {
        const suffix = pathname.slice(prefix.length) || '/'
        const rel = suffix.replace(/^\/+/, '')
        const filePath = safePathWithin(dir, path.join(dir, rel))
        if (filePath && fs.existsSync(filePath) && fs.statSync(filePath).isFile()) {
          sendFile(res, filePath)
          return
        }
        res.writeHead(404)
        return res.end('Not found')
      }
    }

    if (!fs.existsSync(distDir)) {
      res.writeHead(500)
      res.setHeader('Content-Type', 'text/plain; charset=utf-8')
      return res.end(
        'Pasta dist/ inexistente. Execute npm run build na raiz do projeto.'
      )
    }

    if (pathname === '/' || pathname === '') {
      return sendFile(res, path.join(distDir, 'index.html'))
    }

    if (tryStaticFromDir(res, distDir, pathname)) return

    return sendFile(res, path.join(distDir, 'index.html'))
  } catch (e) {
    console.error(e)
    res.writeHead(500)
    res.end('Internal Server Error')
  }
}

const server = http.createServer((req, res) => {
  onRequest(req, res).catch((err) => {
    console.error(err)
    if (!res.headersSent) {
      res.writeHead(500)
      res.end('Internal Server Error')
    }
  })
})

server.listen(PORT, HOST, () => {
  console.log(
    `Portal M.I.S — servidor LAN em http://${HOST === '0.0.0.0' ? '<IP-desta-maquina>' : HOST}:${PORT}/`
  )
  console.log(`Documento raiz: ${rootDir}`)
  console.log(`Para mistrc.com sem porta, use PORT=80 e o ficheiro hosts — ver server/HOSTS-mistrc.txt`)
})
