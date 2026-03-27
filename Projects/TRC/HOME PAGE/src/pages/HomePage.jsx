import { useAuth } from '../auth/AuthContext'

/** Placeholder até a Home completa (grid de relatórios). */
export default function HomePage() {
  const { user, logout } = useAuth()

  return (
    <div
      className="min-h-screen bg-slate-100 p-8 text-slate-900"
      style={{ backgroundImage: "url('/img/fundo_light.png')" }}
    >
      <div className="mx-auto max-w-lg rounded-lg border border-slate-200 bg-white/90 p-6 shadow-sm backdrop-blur-sm">
        <p className="text-lg font-medium">Bem-vindo, {user.nome}</p>
        <p className="mt-1 text-sm text-slate-600">
          Usuário: <span className="font-mono">{user.user}</span> · Papel:{' '}
          <span className="font-mono">{user.role}</span>
        </p>
        <button
          type="button"
          onClick={logout}
          className="mt-6 rounded-md bg-[#910A21] px-4 py-2 text-sm text-white hover:opacity-90"
        >
          Sair
        </button>
      </div>
    </div>
  )
}
