/**
 * Rotas /api/reports e /api/users (mesma lógica que vite.config.js).
 * Usado pelo servidor LAN em produção.
 */
import fs from 'node:fs'
import path from 'node:path'

export function readRequestBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = []
    req.on('data', (c) => chunks.push(c))
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
    req.on('error', reject)
  })
}

export function slugify(s) {
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

function json(res, status, obj) {
  const body = JSON.stringify(obj)
  res.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': Buffer.byteLength(body),
  })
  res.end(body)
}

export function createReportsHandlers(rootDir) {
  const imgDir = path.join(rootDir, 'img')
  const dbPath = path.join(rootDir, 'data', 'db.json')

  async function handlePost(req, res) {
    let payload
    try {
      payload = JSON.parse((await readRequestBody(req)) || '{}')
    } catch {
      return json(res, 400, { error: 'JSON inválido.' })
    }

    const nome = String(payload.nome ?? '').trim()
    const link = String(payload.link ?? '').trim()
    const imageDataUrl = String(payload.imageDataUrl ?? '')
    const m = imageDataUrl.match(
      /^data:(image\/(?:png|jpeg|jpg|webp));base64,(.+)$/i
    )
    if (!nome || !link || !m) {
      return json(res, 400, { error: 'Dados em falta ou imagem inválida.' })
    }
    if (!link.startsWith('https://')) {
      return json(res, 400, { error: 'O link deve ser HTTPS.' })
    }

    let db
    try {
      db = JSON.parse(fs.readFileSync(dbPath, 'utf8'))
    } catch {
      return json(res, 500, { error: 'Ficheiro db.json ilegível.' })
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
      return json(res, 500, { error: 'Não foi possível gravar a imagem.' })
    }

    const report = { id, nome, link, imagePath: `/img/${filename}` }
    db.reports.push(report)
    try {
      fs.writeFileSync(dbPath, `${JSON.stringify(db, null, 2)}\n`)
    } catch {
      return json(res, 500, { error: 'Não foi possível atualizar db.json.' })
    }

    return json(res, 201, { ok: true, report })
  }

  async function handleDelete(req, res) {
    let payload
    try {
      payload = JSON.parse((await readRequestBody(req)) || '{}')
    } catch {
      return json(res, 400, { error: 'JSON inválido.' })
    }
    const id = Number(payload.id)
    if (!Number.isFinite(id)) {
      return json(res, 400, { error: 'ID inválido.' })
    }

    let db
    try {
      db = JSON.parse(fs.readFileSync(dbPath, 'utf8'))
    } catch {
      return json(res, 500, { error: 'Ficheiro db.json ilegível.' })
    }
    if (!Array.isArray(db.reports)) db.reports = []
    const idx = db.reports.findIndex((r) => Number(r.id) === id)
    if (idx === -1) {
      return json(res, 404, { error: 'Relatório não encontrado.' })
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
          /* ignore */
        }
      }
    }
    try {
      fs.writeFileSync(dbPath, `${JSON.stringify(db, null, 2)}\n`)
    } catch {
      return json(res, 500, { error: 'Não foi possível atualizar db.json.' })
    }
    return json(res, 200, { ok: true })
  }

  return { handlePost, handleDelete }
}

function normalizeUserRole(r) {
  return r === 'admin' ? 'admin' : 'user'
}

export function createUsersHandlers(rootDir) {
  const dbPath = path.join(rootDir, 'data', 'db.json')

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
      payload = JSON.parse((await readRequestBody(req)) || '{}')
    } catch {
      return json(res, 400, { error: 'JSON inválido.' })
    }

    const login = String(payload.user ?? '').trim().toLowerCase()
    const nome = String(payload.nome ?? '').trim()
    const senha = String(payload.senha ?? '')
    const role = normalizeUserRole(payload.role)

    if (!login || !nome || !senha) {
      return json(res, 400, { error: 'Preencha utilizador, nome e senha.' })
    }

    let db
    try {
      db = loadDb()
    } catch {
      return json(res, 500, { error: 'db.json ilegível.' })
    }

    if (!Array.isArray(db.users)) db.users = []
    if (
      db.users.some((u) => String(u.user ?? u.nome).toLowerCase() === login)
    ) {
      return json(res, 400, { error: 'Já existe utilizador com esse login.' })
    }

    const id = Date.now()
    const row = { id, user: login, nome, senha, role }
    db.users.push(row)
    try {
      saveDb(db)
    } catch {
      return json(res, 500, { error: 'Não foi possível gravar db.json.' })
    }

    return json(res, 201, {
      ok: true,
      user: { id: row.id, user: row.user, nome: row.nome, role: row.role },
    })
  }

  async function handlePatch(req, res) {
    let payload
    try {
      payload = JSON.parse((await readRequestBody(req)) || '{}')
    } catch {
      return json(res, 400, { error: 'JSON inválido.' })
    }

    const id = Number(payload.id)
    if (!Number.isFinite(id)) {
      return json(res, 400, { error: 'ID inválido.' })
    }

    let db
    try {
      db = loadDb()
    } catch {
      return json(res, 500, { error: 'db.json ilegível.' })
    }

    if (!Array.isArray(db.users)) db.users = []
    const idx = db.users.findIndex((u) => Number(u.id) === id)
    if (idx === -1) {
      return json(res, 404, { error: 'Utilizador não encontrado.' })
    }

    const u = { ...db.users[idx] }

    if (payload.role !== undefined && payload.role !== null) {
      const nextRole = normalizeUserRole(payload.role)
      if (
        u.role === 'admin' &&
        nextRole === 'user' &&
        adminCount(db.users) <= 1
      ) {
        return json(res, 400, {
          error: 'Tem de existir pelo menos um administrador.',
        })
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
      return json(res, 500, { error: 'Não foi possível gravar db.json.' })
    }

    return json(res, 200, {
      ok: true,
      user: { id: u.id, user: u.user, nome: u.nome, role: u.role },
    })
  }

  async function handleDelete(req, res) {
    let payload
    try {
      payload = JSON.parse((await readRequestBody(req)) || '{}')
    } catch {
      return json(res, 400, { error: 'JSON inválido.' })
    }

    const id = Number(payload.id)
    if (!Number.isFinite(id)) {
      return json(res, 400, { error: 'ID inválido.' })
    }

    let db
    try {
      db = loadDb()
    } catch {
      return json(res, 500, { error: 'db.json ilegível.' })
    }

    if (!Array.isArray(db.users)) db.users = []
    const idx = db.users.findIndex((u) => Number(u.id) === id)
    if (idx === -1) {
      return json(res, 404, { error: 'Utilizador não encontrado.' })
    }

    const u = db.users[idx]
    if (u.role === 'admin' && adminCount(db.users) <= 1) {
      return json(res, 400, {
        error: 'Não pode remover o único administrador.',
      })
    }

    db.users.splice(idx, 1)
    try {
      saveDb(db)
    } catch {
      return json(res, 500, { error: 'Não foi possível gravar db.json.' })
    }

    return json(res, 200, { ok: true })
  }

  return { handlePost, handlePatch, handleDelete }
}
