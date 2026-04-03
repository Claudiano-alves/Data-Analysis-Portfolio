import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'
import react from '@vitejs/plugin-react'
import sirv from 'sirv'
import { defineConfig } from 'vite'
import { viteStaticCopy } from 'vite-plugin-static-copy'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

function slugify(s) {
  return (
    String(s)
      .normalize('NFD')
      .replace(/[\u0300-\u036f]/g, '')
      .replace(/[^a-zA-Z0-9]+/g, '-')
      .replace(/^-|-$/g, '')
      .slice(0, 48)
      .toLowerCase() || 'relatorio'
  )
}

function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    req.on('data', (c) => chunks.push(c))
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
    req.on('error', reject)
  })
}

/** POST /api/reports — grava imagem em /img e atualiza data/db.json (apenas dev/preview). */
function reportsApiPlugin() {
  const imgDir = path.resolve(__dirname, 'img')
  const dataDir = path.resolve(__dirname, 'data')
  const dbPath = path.join(dataDir, 'db.json')

  async function handlePost(req, res) {
    let body
    try {
      body = await readRequestBody(req)
    } catch {
      res.statusCode = 400
      res.end()
      return
    }
    let payload
    try {
      payload = JSON.parse(body || '{}')
    } catch {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'JSON inválido.' }))
      return
    }

    const nome = String(payload.nome ?? '').trim()
    const link = String(payload.link ?? '').trim()
    const imageDataUrl = String(payload.imageDataUrl ?? '')

    const m = imageDataUrl.match(
      /^data:(image\/(?:png|jpeg|jpg|webp));base64,(.+)$/i
    )
    if (!nome || !link || !m) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Dados em falta ou imagem inválida.' }))
      return
    }

    if (!link.startsWith('https://')) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'O link deve ser HTTPS.' }))
      return
    }

    let db
    try {
      db = JSON.parse(fs.readFileSync(dbPath, 'utf8'))
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Ficheiro db.json ilegível.' }))
      return
    }

    if (!Array.isArray(db.reports)) db.reports = []

    const rawExt = m[1].replace('image/', '').toLowerCase()
    const ext = rawExt === 'jpeg' ? 'jpg' : rawExt
    const buf = Buffer.from(m[2], 'base64')
    const id = Date.now()
    const filename = `${slugify(nome)}-${id}.${ext}`

    try {
      fs.mkdirSync(imgDir, { recursive: true })
      fs.writeFileSync(path.join(imgDir, filename), buf)
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Não foi possível gravar a imagem.' }))
      return
    }

    const report = {
      id,
      nome,
      link,
      imagePath: `/img/${filename}`,
    }
    db.reports.push(report)
    try {
      fs.writeFileSync(dbPath, `${JSON.stringify(db, null, 2)}\n`)
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Não foi possível atualizar db.json.' }))
      return
    }

    res.statusCode = 201
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify({ ok: true, report }))
  }

  async function handleDelete(req, res) {
    let body
    try {
      body = await readRequestBody(req)
    } catch {
      res.statusCode = 400
      res.end()
      return
    }
    let payload
    try {
      payload = JSON.parse(body || '{}')
    } catch {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'JSON inválido.' }))
      return
    }

    const id = Number(payload.id)
    if (!Number.isFinite(id)) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'ID inválido.' }))
      return
    }

    let db
    try {
      db = JSON.parse(fs.readFileSync(dbPath, 'utf8'))
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Ficheiro db.json ilegível.' }))
      return
    }

    if (!Array.isArray(db.reports)) db.reports = []
    const idx = db.reports.findIndex((r) => Number(r.id) === id)
    if (idx === -1) {
      res.statusCode = 404
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Relatório não encontrado.' }))
      return
    }

    const removed = db.reports[idx]
    db.reports.splice(idx, 1)

    const p = String(removed.imagePath ?? '')
    if (p.startsWith('/img/')) {
      const fname = path.basename(p)
      if (fname && fname !== '.' && fname !== '..') {
        try {
          fs.unlinkSync(path.join(imgDir, fname))
        } catch {
          /* ficheiro ausente ou não removível */
        }
      }
    }

    try {
      fs.writeFileSync(dbPath, `${JSON.stringify(db, null, 2)}\n`)
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Não foi possível atualizar db.json.' }))
      return
    }

    res.statusCode = 200
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify({ ok: true }))
  }

  function attach(server) {
    server.middlewares.use(async (req, res, next) => {
      const pathname = req.url?.split('?')[0] ?? ''
      if (pathname !== '/api/reports') {
        return next()
      }
      if (req.method === 'POST') {
        await handlePost(req, res)
        return
      }
      if (req.method === 'DELETE') {
        await handleDelete(req, res)
        return
      }
      next()
    })
  }

  return {
    name: 'portal-reports-api',
    configureServer: attach,
    configurePreviewServer: attach,
  }
}

function normalizeUserRole(r) {
  return r === 'admin' ? 'admin' : 'user'
}

/** POST/PATCH/DELETE /api/users — atualiza data/db.json (dev/preview). */
function usersApiPlugin() {
  const dataDir = path.resolve(__dirname, 'data')
  const dbPath = path.join(dataDir, 'db.json')

  function loadDb() {
    return JSON.parse(fs.readFileSync(dbPath, 'utf8'))
  }

  function saveDb(db) {
    fs.writeFileSync(dbPath, `${JSON.stringify(db, null, 2)}\n`)
  }

  function adminCount(users) {
    if (!Array.isArray(users)) return 0
    return users.filter((u) => u.role === 'admin').length
  }

  async function handlePost(req, res) {
    let payload
    try {
      payload = JSON.parse(await readRequestBody(req) || '{}')
    } catch {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'JSON inválido.' }))
      return
    }

    const login = String(payload.user ?? '').trim().toLowerCase()
    const nome = String(payload.nome ?? '').trim()
    const senha = String(payload.senha ?? '')
    const role = normalizeUserRole(payload.role)

    if (!login || !nome || !senha) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Preencha utilizador, nome e senha.' }))
      return
    }

    let db
    try {
      db = loadDb()
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'db.json ilegível.' }))
      return
    }

    if (!Array.isArray(db.users)) db.users = []
    const dup = db.users.some(
      (u) => String(u.user ?? u.nome).toLowerCase() === login
    )
    if (dup) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Já existe utilizador com esse login.' }))
      return
    }

    const id = Date.now()
    const row = { id, user: login, nome, senha, role }
    db.users.push(row)
    try {
      saveDb(db)
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Não foi possível gravar db.json.' }))
      return
    }

    res.statusCode = 201
    res.setHeader('Content-Type', 'application/json')
    res.end(
      JSON.stringify({
        ok: true,
        user: { id: row.id, user: row.user, nome: row.nome, role: row.role },
      })
    )
  }

  async function handlePatch(req, res) {
    let payload
    try {
      payload = JSON.parse(await readRequestBody(req) || '{}')
    } catch {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'JSON inválido.' }))
      return
    }

    const id = Number(payload.id)
    if (!Number.isFinite(id)) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'ID inválido.' }))
      return
    }

    let db
    try {
      db = loadDb()
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'db.json ilegível.' }))
      return
    }

    if (!Array.isArray(db.users)) db.users = []
    const idx = db.users.findIndex((u) => Number(u.id) === id)
    if (idx === -1) {
      res.statusCode = 404
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Utilizador não encontrado.' }))
      return
    }

    const u = { ...db.users[idx] }

    if (payload.role !== undefined && payload.role !== null) {
      const nextRole = normalizeUserRole(payload.role)
      if (
        u.role === 'admin' &&
        nextRole === 'user' &&
        adminCount(db.users) <= 1
      ) {
        res.statusCode = 400
        res.setHeader('Content-Type', 'application/json')
        res.end(
          JSON.stringify({ error: 'Tem de existir pelo menos um administrador.' })
        )
        return
      }
      u.role = nextRole
    }

    if (payload.nome !== undefined && String(payload.nome).trim()) {
      u.nome = String(payload.nome).trim()
    }
    if (payload.senha !== undefined && String(payload.senha).length > 0) {
      u.senha = String(payload.senha)
    }

    db.users[idx] = u
    try {
      saveDb(db)
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Não foi possível gravar db.json.' }))
      return
    }

    res.statusCode = 200
    res.setHeader('Content-Type', 'application/json')
    res.end(
      JSON.stringify({
        ok: true,
        user: {
          id: u.id,
          user: u.user,
          nome: u.nome,
          role: u.role,
        },
      })
    )
  }

  async function handleDelete(req, res) {
    let payload
    try {
      payload = JSON.parse(await readRequestBody(req) || '{}')
    } catch {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'JSON inválido.' }))
      return
    }

    const id = Number(payload.id)
    if (!Number.isFinite(id)) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'ID inválido.' }))
      return
    }

    let db
    try {
      db = loadDb()
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'db.json ilegível.' }))
      return
    }

    if (!Array.isArray(db.users)) db.users = []
    const idx = db.users.findIndex((u) => Number(u.id) === id)
    if (idx === -1) {
      res.statusCode = 404
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Utilizador não encontrado.' }))
      return
    }

    const u = db.users[idx]
    if (u.role === 'admin' && adminCount(db.users) <= 1) {
      res.statusCode = 400
      res.setHeader('Content-Type', 'application/json')
      res.end(
        JSON.stringify({ error: 'Não pode remover o único administrador.' })
      )
      return
    }

    db.users.splice(idx, 1)
    try {
      saveDb(db)
    } catch {
      res.statusCode = 500
      res.setHeader('Content-Type', 'application/json')
      res.end(JSON.stringify({ error: 'Não foi possível gravar db.json.' }))
      return
    }

    res.statusCode = 200
    res.setHeader('Content-Type', 'application/json')
    res.end(JSON.stringify({ ok: true }))
  }

  function attach(server) {
    server.middlewares.use(async (req, res, next) => {
      const pathname = req.url?.split('?')[0] ?? ''
      if (pathname !== '/api/users') {
        return next()
      }
      if (req.method === 'POST') {
        await handlePost(req, res)
        return
      }
      if (req.method === 'PATCH') {
        await handlePatch(req, res)
        return
      }
      if (req.method === 'DELETE') {
        await handleDelete(req, res)
        return
      }
      next()
    })
  }

  return {
    name: 'portal-users-api',
    configureServer: attach,
    configurePreviewServer: attach,
  }
}

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
    reportsApiPlugin(),
    usersApiPlugin(),
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
