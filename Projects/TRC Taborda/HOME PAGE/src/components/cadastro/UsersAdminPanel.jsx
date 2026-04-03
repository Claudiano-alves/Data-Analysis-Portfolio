import { useCallback, useEffect, useState } from 'react'
import { useAuth } from '../../auth/AuthContext'

const BTN = 'rounded-[11.55px]'
const FIELD = 'rounded-[8.4px]'

export default function UsersAdminPanel({ onBack }) {
  const { user: session, refreshUser } = useAuth()
  const [rows, setRows] = useState([])
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')

  const [novoLogin, setNovoLogin] = useState('')
  const [novoNome, setNovoNome] = useState('')
  const [novoSenha, setNovoSenha] = useState('')
  const [novoRole, setNovoRole] = useState('user')

  const load = useCallback(async () => {
    setError('')
    try {
      const res = await fetch('/data/db.json', { cache: 'no-store' })
      const data = await res.json()
      const list = Array.isArray(data.users) ? [...data.users] : []
      list.sort((a, b) => Number(a.id) - Number(b.id))
      setRows(list)
    } catch {
      setError('Não foi possível carregar os utilizadores.')
      setRows([])
    }
  }, [])

  useEffect(() => {
    load()
  }, [load])

  async function criarUsuario(e) {
    e.preventDefault()
    setError('')
    const login = novoLogin.trim().toLowerCase()
    const nome = novoNome.trim()
    const senha = novoSenha
    if (!login || !nome || !senha) {
      setError('Preencha utilizador, nome e senha.')
      return
    }
    setBusy(true)
    try {
      const res = await fetch('/api/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ user: login, nome, senha, role: novoRole }),
      })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) {
        setError(body.error || 'Não foi possível criar o utilizador.')
        return
      }
      setNovoLogin('')
      setNovoNome('')
      setNovoSenha('')
      setNovoRole('user')
      await load()
    } catch {
      setError('API indisponível. Use npm run dev para gravar no JSON.')
    } finally {
      setBusy(false)
    }
  }

  async function alterarRole(u, role) {
    if (u.role === role) return
    setError('')
    setBusy(true)
    try {
      const res = await fetch('/api/users', {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: u.id, role }),
      })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) {
        setError(body.error || 'Não foi possível atualizar o acesso.')
        setBusy(false)
        await load()
        return
      }
      await load()
      if (Number(session?.id) === Number(u.id)) await refreshUser()
    } catch {
      setError('API indisponível.')
    } finally {
      setBusy(false)
    }
  }

  async function removerUsuario(u) {
    if (
      !window.confirm(
        `Remover definitivamente o utilizador «${u.user ?? u.nome}»?`
      )
    )
      return
    setError('')
    setBusy(true)
    try {
      const res = await fetch('/api/users', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: u.id }),
      })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) {
        setError(body.error || 'Não foi possível remover.')
        setBusy(false)
        return
      }
      await load()
      if (Number(session?.id) === Number(u.id)) {
        // sessão invalidada no servidor — sair
        window.location.reload()
      }
    } catch {
      setError('API indisponível.')
    } finally {
      setBusy(false)
    }
  }

  const adminCount = rows.filter((u) => u.role === 'admin').length

  return (
    <div className="mx-auto max-w-3xl">
      <div className="mb-6 flex flex-wrap items-center justify-between gap-4">
        <h1 className="font-arima text-xl font-semibold text-[#910A21]">
          Cadastro de utilizadores
        </h1>
        <button
          type="button"
          onClick={onBack}
          className={`${BTN} border border-[#910A21] px-4 py-2 font-arima text-sm font-medium text-[#910A21] hover:bg-[#910A21]/10`}
        >
          Voltar à home
        </button>
      </div>

      <p className="mb-6 text-sm text-gray-600">
        Administradores podem incluir e remover relatórios e gerir utilizadores. Utilizadores
        simples acedem apenas aos relatórios.
      </p>

      {error ? (
        <p className="mb-4 text-sm text-red-600" role="alert">
          {error}
        </p>
      ) : null}

      <form
        onSubmit={criarUsuario}
        className="mb-8 rounded-[12.1px] border border-[#910A21]/20 bg-white/70 p-4 shadow-sm backdrop-blur-sm"
      >
        <h2 className="font-arima text-sm font-semibold text-[#910A21]">
          Novo utilizador
        </h2>
        <div className="mt-3 grid gap-3 sm:grid-cols-2">
          <label className="block text-xs text-gray-700">
            Utilizador (login)
            <input
              value={novoLogin}
              onChange={(e) => setNovoLogin(e.target.value)}
              className={`mt-1 w-full border border-gray-200 px-2 py-2 text-sm outline-none ring-[#910A21]/25 focus:ring-2 ${FIELD}`}
              autoComplete="off"
            />
          </label>
          <label className="block text-xs text-gray-700">
            Nome
            <input
              value={novoNome}
              onChange={(e) => setNovoNome(e.target.value)}
              className={`mt-1 w-full border border-gray-200 px-2 py-2 text-sm outline-none ring-[#910A21]/25 focus:ring-2 ${FIELD}`}
              autoComplete="off"
            />
          </label>
          <label className="block text-xs text-gray-700">
            Senha
            <input
              type="password"
              value={novoSenha}
              onChange={(e) => setNovoSenha(e.target.value)}
              className={`mt-1 w-full border border-gray-200 px-2 py-2 text-sm outline-none ring-[#910A21]/25 focus:ring-2 ${FIELD}`}
              autoComplete="new-password"
            />
          </label>
          <label className="block text-xs text-gray-700">
            Tipo de acesso
            <select
              value={novoRole}
              onChange={(e) => setNovoRole(e.target.value)}
              className={`mt-1 w-full border border-gray-200 px-2 py-2 text-sm outline-none ring-[#910A21]/25 focus:ring-2 ${FIELD}`}
            >
              <option value="user">Utilizador simples</option>
              <option value="admin">Administrador</option>
            </select>
          </label>
        </div>
        <button
          type="submit"
          disabled={busy}
          className={`${BTN} mt-4 bg-[#910A21] px-5 py-2 font-arima text-sm font-medium text-white disabled:opacity-50`}
        >
          Criar utilizador
        </button>
      </form>

      <div className="rounded-[12.1px] border border-gray-200/80 bg-white/70 shadow-sm backdrop-blur-sm">
        <h2 className="border-b border-gray-200/80 px-4 py-3 font-arima text-sm font-semibold text-[#910A21]">
          Utilizadores existentes ({rows.length})
        </h2>
        <div className="home-frame-scroll max-h-[min(420px,50vh)] overflow-x-auto overflow-y-auto">
          <table className="w-full min-w-[520px] text-left text-sm">
            <thead>
              <tr className="border-b border-gray-100 bg-[#910A21]/5 text-xs uppercase tracking-wide text-gray-600">
                <th className="px-4 py-3 font-arima font-medium">Login</th>
                <th className="px-4 py-3 font-arima font-medium">Nome</th>
                <th className="px-4 py-3 font-arima font-medium">Acesso</th>
                <th className="px-4 py-3 font-arima font-medium">Ações</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((u) => {
                const isSelf = Number(session?.id) === Number(u.id)
                return (
                  <tr
                    key={u.id}
                    className="border-b border-gray-100 last:border-0 hover:bg-white/50"
                  >
                    <td className="px-4 py-3 font-mono text-gray-900">
                      {u.user ?? u.nome}
                    </td>
                    <td className="px-4 py-3 text-gray-800">{u.nome}</td>
                    <td className="px-4 py-3">
                      <select
                        value={u.role === 'admin' ? 'admin' : 'user'}
                        disabled={busy || (u.role === 'admin' && adminCount <= 1)}
                        title={
                          u.role === 'admin' && adminCount <= 1
                            ? 'Tem de existir pelo menos um administrador.'
                            : undefined
                        }
                        onChange={(e) => alterarRole(u, e.target.value)}
                        className={`max-w-[200px] border border-gray-200 px-2 py-1.5 text-sm ${FIELD}`}
                      >
                        <option value="user">Utilizador simples</option>
                        <option value="admin">Administrador</option>
                      </select>
                    </td>
                    <td className="px-4 py-3">
                      <button
                        type="button"
                        disabled={
                          busy ||
                          (u.role === 'admin' && adminCount <= 1)
                        }
                        onClick={() => removerUsuario(u)}
                        className={`${BTN} text-xs text-red-700 underline-offset-2 hover:underline disabled:cursor-not-allowed disabled:opacity-40`}
                        title={
                          u.role === 'admin' && adminCount <= 1
                            ? 'Não pode remover o único administrador.'
                            : 'Remover utilizador'
                        }
                      >
                        Excluir
                      </button>
                      {isSelf ? (
                        <span className="ml-2 text-xs text-gray-500">(é você)</span>
                      ) : null}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
