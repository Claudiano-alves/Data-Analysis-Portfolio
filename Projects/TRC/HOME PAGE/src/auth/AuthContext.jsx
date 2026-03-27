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
      role: row.role,
    }
    sessionStorage.setItem('portal-auth', JSON.stringify(session))
    setUser(session)
    return { ok: true }
  }, [])

  const logout = useCallback(() => {
    sessionStorage.removeItem('portal-auth')
    setUser(null)
  }, [])

  const value = useMemo(
    () => ({ user, ready, login, logout }),
    [user, ready, login, logout]
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
