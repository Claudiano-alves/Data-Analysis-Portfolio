import { useState } from 'react'
import { useAuth } from '../auth/AuthContext'

export default function LoginPage() {
  const { login } = useAuth()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState(false)
  const [busy, setBusy] = useState(false)

  async function handleSubmit(e) {
    e.preventDefault()
    setError(false)
    setBusy(true)
    try {
      const result = await login(username.trim(), password)
      if (!result.ok) setError(true)
    } catch {
      setError(true)
    } finally {
      setBusy(false)
    }
  }

  return (
    <div
      className="relative min-h-screen w-full bg-cover bg-fixed bg-center bg-no-repeat"
      style={{ backgroundImage: "url('/img/fundo_light.png')" }}
    >
      <div className="flex min-h-screen flex-col items-center justify-center px-4 pb-24 pt-8">
        <div
          className="relative flex w-[500px] max-w-full flex-shrink-0 flex-col overflow-hidden"
          style={{
            height: 350,
            borderRadius: 50,
            boxShadow:
              '0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -2px rgba(0, 0, 0, 0.1)',
          }}
        >
          <div className="absolute inset-0 overflow-hidden rounded-[50px]">
            <img
              src="/img/fundo_burgundy.png"
              alt=""
              className="h-full w-full object-cover"
              draggable={false}
            />
            {/* Linear #910A21 — camada com 70% opacidade (≈30% transparente); ajuste se o handoff for o inverso */}
            <div
              className="pointer-events-none absolute inset-0 rounded-[50px]"
              style={{
                background:
                  'linear-gradient(180deg, rgba(145, 10, 33, 0.7) 0%, rgba(145, 10, 33, 0.7) 100%)',
              }}
              aria-hidden
            />
          </div>

          <form
            onSubmit={handleSubmit}
            className="relative z-10 flex h-full flex-col items-center justify-start px-8 pb-6 pt-[105px]"
          >
            <div className="flex items-center justify-center gap-[13px]">
              <img
                src="/icons/icon_bi_light.png"
                alt=""
                width={33}
                height={33}
                className="shrink-0"
              />
              <span className="font-arima text-[22px] font-medium leading-none text-white/80">
                M.I.S
              </span>
              <img
                src="/logos_carteiras/trc_light.png"
                alt="TRC"
                className="h-auto max-h-[0.5808rem] w-auto shrink-0 self-center object-contain opacity-80"
              />
            </div>

            <div className="mt-[2.592rem] flex w-full max-w-[153.6px] flex-col items-center gap-[0.825rem]">
              <div className="relative w-full">
                <label htmlFor="login-user" className="sr-only">
                  User
                </label>
                <img
                  src="/icons/icon_user.png"
                  alt=""
                  width={11}
                  height={11}
                  className="pointer-events-none absolute left-3 top-1/2 size-[11px] -translate-y-1/2 opacity-25"
                />
                <input
                  id="login-user"
                  name="user"
                  autoComplete="username"
                  placeholder="User"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  className="h-[1.54rem] w-full rounded-[8.32px] border-0 bg-white pl-8 pr-3 font-arimo text-xs text-gray-600 outline-none ring-0 placeholder:font-arimo placeholder:text-black/20 focus:ring-2 focus:ring-white/40"
                />
              </div>

              <div className="relative w-full">
                <label htmlFor="login-password" className="sr-only">
                  Password
                </label>
                <img
                  src="/icons/passwors_user.png"
                  alt=""
                  width={11}
                  height={11}
                  className="pointer-events-none absolute left-3 top-1/2 size-[11px] -translate-y-1/2 opacity-25"
                />
                <input
                  id="login-password"
                  name="password"
                  type="password"
                  autoComplete="current-password"
                  placeholder="Password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="h-[1.54rem] w-full rounded-[8.32px] border-0 bg-white pl-8 pr-3 font-arimo text-xs text-gray-600 outline-none ring-0 placeholder:font-arimo placeholder:text-black/20 focus:ring-2 focus:ring-white/40"
                />
              </div>
            </div>

            <div className="min-h-8 flex-1 shrink-0" aria-hidden />

            <div className="flex w-full flex-col items-center gap-2">
              <button
                type="submit"
                disabled={busy}
                aria-label="Acessar o portal"
                className="group relative flex items-center justify-center overflow-hidden border-0 text-[11px] font-medium lowercase leading-none text-white outline-none transition-[box-shadow] focus-visible:ring-2 focus-visible:ring-white/60 focus-visible:ring-offset-0"
                style={{
                  width: 60,
                  height: 24,
                  borderRadius: 7,
                  boxShadow:
                    '0 1px 3px 0 rgb(0 0 0 / 0.1), 0 1px 2px -1px rgb(0 0 0 / 0.1)',
                }}
              >
                <span
                  className="pointer-events-none absolute inset-0 bg-cover bg-center opacity-70 transition-opacity group-hover:opacity-85 group-disabled:opacity-45"
                  style={{
                    backgroundImage: "url('/img/fundo_burgundy.png')",
                    backgroundSize: 'cover',
                    backgroundPosition: 'center',
                  }}
                  aria-hidden
                />
                <span className="relative z-[1] leading-none">acess</span>
              </button>
              {error ? (
                <p className="text-center text-xs text-white/90" role="alert">
                  Usuário ou senha inválidos.
                </p>
              ) : null}
            </div>
          </form>
        </div>
      </div>

      <div className="pointer-events-none absolute bottom-6 left-1/2 -translate-x-1/2">
        <img
          src="/logos_carteiras/logo_trc_burgundy.png"
          alt="TRC"
          width={40}
          height={17}
          className="h-[17px] w-10 object-contain"
        />
      </div>
    </div>
  )
}
