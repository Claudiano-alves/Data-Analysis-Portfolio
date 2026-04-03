import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react'

const AuthContext = createContext(null)

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null)
  const [ready, setReady] = useState(false)

  useEffect(() => {
    try {
      const raw = sessionStorage.getItem('portal-auth')
      if (raw) setUser(JSON.parse(raw))
    } catch {
      sessionStorage.removeItem('portal-auth')
    }
    setReady(true)
  }, [])

  const login = useCallback(async (username, password) => {
    const res = await fetch('/data/db.json', { cache: 'no-store' })
    if (!res.ok) return { ok: false, error: 'network' }
    const data = await res.json()
    const row = data.users?.find(
      (u) =>
        String(u.user ?? u.nome).toLowerCase() === String(username).toLowerCase() &&
        u.senha === password
    )
    if (!row) return { ok: false, error: 'invalid' }
    const session = {
      id: row.id,
      user: row.user ?? row.nome,
      nome: row.nome,
      role: row.role === 'admin' ? 'admin' : 'user',
    }
    sessionStorage.setItem('portal-auth', JSON.stringify(session))
    setUser(session)
    return { ok: true }
  }, [])

  const logout = useCallback(() => {
    sessionStorage.removeItem('portal-auth')
    setUser(null)
  }, [])

  /** Atualiza nome/papel na sessão a partir do db.json (ex.: após editar utilizador). */
  const refreshUser = useCallback(async () => {
    const raw = sessionStorage.getItem('portal-auth')
    if (!raw) return
    let session
    try {
      session = JSON.parse(raw)
    } catch {
      return
    }
    const res = await fetch('/data/db.json', { cache: 'no-store' })
    if (!res.ok) return
    const data = await res.json()
    const row = data.users?.find((u) => Number(u.id) === Number(session.id))
    if (!row) {
      sessionStorage.removeItem('portal-auth')
      setUser(null)
      return
    }
    const next = {
      id: row.id,
      user: row.user ?? row.nome,
      nome: row.nome,
      role: row.role === 'admin' ? 'admin' : 'user',
    }
    sessionStorage.setItem('portal-auth', JSON.stringify(next))
    setUser(next)
  }, [])

  const value = useMemo(
    () => ({ user, ready, login, logout, refreshUser }),
    [user, ready, login, logout, refreshUser]
  )

  return (
    <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
  )
}

export function useAuth() {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth deve ser usado dentro de AuthProvider')
  return ctx
}
