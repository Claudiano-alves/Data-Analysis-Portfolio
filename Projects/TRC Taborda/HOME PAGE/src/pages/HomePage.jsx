import { Plus } from 'lucide-react'
import { useCallback, useEffect, useState } from 'react'
import { useAuth } from '../auth/AuthContext'
import UsersAdminPanel from '../components/cadastro/UsersAdminPanel'
import AddReportModal from '../components/home/AddReportModal'
import FullScreenReport from '../components/home/FullScreenReport'
import { FIXED_EMBEDS } from '../constants/powerBi'

/** Botões: raio base 11px +5% */
const BTN_RADIUS = 'rounded-[11.55px]'
/** Cartões de relatório: raio base 11px +10% */
const CARD_RADIUS = 'rounded-[12.1px]'

/**
 * Painel principal: Home | Cadastro (só admin) | embeds TRC/Ranking/card.
 * Scroll só na área do grid (barra personalizada); documento sem scroll.
 */
export default function HomePage() {
  const { logout, user } = useAuth()
  const isAdmin = user?.role === 'admin'
  const [panel, setPanel] = useState('home')
  const [reports, setReports] = useState([])
  const [modalOpen, setModalOpen] = useState(false)
  const [cardReport, setCardReport] = useState(null)

  const loadReports = useCallback(async () => {
    try {
      const res = await fetch('/data/db.json', { cache: 'no-store' })
      const data = await res.json()
      setReports(Array.isArray(data.reports) ? data.reports : [])
    } catch {
      setReports([])
    }
  }, [])

  useEffect(() => {
    loadReports()
  }, [loadReports])

  useEffect(() => {
    if (panel === 'cadastro' && !isAdmin) setPanel('home')
  }, [panel, isAdmin])

  const fullscreen =
    panel === 'trc' || panel === 'ranking' || (panel === 'report' && cardReport)

  function goHome() {
    setPanel('home')
    setCardReport(null)
  }

  async function removeReport(r, ev) {
    ev.preventDefault()
    ev.stopPropagation()
    if (!isAdmin) return
    const ok = window.confirm(`Remover o relatório «${r.nome}»?`)
    if (!ok) return
    try {
      const res = await fetch('/api/reports', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ id: r.id }),
      })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) {
        window.alert(body.error || 'Não foi possível remover o relatório.')
        return
      }
      await loadReports()
    } catch {
      window.alert(
        'Em build estático use npm run dev para remover ficheiros e atualizar o JSON.'
      )
    }
  }

  function navButtonProps(id) {
    const active = panel === id
    return {
      type: 'button',
      className: `${BTN_RADIUS} font-arima px-3 py-1.5 text-sm font-medium transition-colors ${
        active
          ? 'border border-[#910A21] text-[#910A21] shadow-sm'
          : 'border border-transparent text-[#910A21] hover:bg-[#910A21]/10'
      }`,
    }
  }

  return (
    <div
      className="flex h-screen flex-col overflow-hidden bg-cover bg-fixed bg-center bg-no-repeat"
      style={{ backgroundImage: "url('/img/fundo_light.png')" }}
    >
      {!fullscreen ? (
        <header className="grid shrink-0 grid-cols-[1fr_auto_1fr] items-center gap-4 border-b border-white/25 bg-white/20 px-4 py-3 shadow-sm backdrop-blur-xl supports-[backdrop-filter]:bg-white/15 md:px-8">
          <nav className="flex flex-wrap items-center gap-2 justify-self-start">
            <button {...navButtonProps('home')} onClick={goHome}>
              Home
            </button>
            {isAdmin ? (
              <button
                {...navButtonProps('cadastro')}
                onClick={() => {
                  setPanel('cadastro')
                  setCardReport(null)
                }}
              >
                Cadastro
              </button>
            ) : null}
            <button
              {...navButtonProps('trc')}
              onClick={() => {
                setPanel('trc')
                setCardReport(null)
              }}
            >
              TRC
            </button>
            <button
              {...navButtonProps('ranking')}
              onClick={() => {
                setPanel('ranking')
                setCardReport(null)
              }}
            >
              Ranking
            </button>
          </nav>

          <div className="flex items-center justify-center gap-[13px] justify-self-center">
            <img
              src="/icons/icon_bi_burgundy.png"
              alt=""
              width={33}
              height={33}
              className="shrink-0"
            />
            <span className="font-arima text-[22px] font-semibold leading-none text-[#910A21]">
              M.I.S
            </span>
            <img
              src="/logos_carteiras/logo_trc_burgundy.png"
              alt="TRC"
              className="h-auto max-h-[7.92px] w-auto shrink-0 object-contain"
            />
          </div>

          <div className="flex justify-end justify-self-end">
            <button
              type="button"
              onClick={logout}
              title="Terminar sessão e voltar ao ecrã de login"
              className={`${BTN_RADIUS} font-arima px-3 py-1.5 text-sm font-medium text-[#910A21] hover:bg-[#910A21]/10`}
            >
              Login
            </button>
          </div>
        </header>
      ) : null}

      {!fullscreen ? (
        <main className="home-frame-scroll min-h-0 flex-1 overflow-y-auto overscroll-contain px-4 py-6 md:px-10">
          {panel === 'home' ? (
            <div className="mx-auto max-w-5xl">
              <div className="grid grid-cols-1 gap-6 sm:grid-cols-2 lg:grid-cols-3">
                {reports.map((r) => (
                  <div
                    key={r.id}
                    className={`group relative aspect-[16/10] w-full overflow-hidden ${CARD_RADIUS} border border-gray-200/80 bg-gray-100 text-left shadow-md transition hover:shadow-lg`}
                  >
                    <button
                      type="button"
                      className="absolute inset-0 z-[1] focus-visible:outline focus-visible:ring-2 focus-visible:ring-[#910A21]/40 focus-visible:ring-offset-2"
                      aria-label={`Abrir relatório ${r.nome}`}
                      onClick={() => {
                        setCardReport(r)
                        setPanel('report')
                      }}
                    />
                    <img
                      src={r.imagePath}
                      alt=""
                      className="pointer-events-none absolute inset-0 z-0 h-full w-full object-cover transition group-hover:scale-[1.02]"
                    />
                    <div className="pointer-events-none absolute inset-0 z-[1] bg-gradient-to-t from-black/75 via-black/25 to-transparent" />
                    {isAdmin ? (
                      <button
                        type="button"
                        title="Remover relatório"
                        aria-label={`Remover relatório ${r.nome}`}
                        className="absolute right-2 top-2 z-20 inline-block border-0 bg-transparent p-0 leading-none shadow-none ring-0 focus-visible:outline focus-visible:ring-2 focus-visible:ring-[#910A21]/60 focus-visible:ring-offset-1"
                        onClick={(ev) => removeReport(r, ev)}
                      >
                        <img
                          src="/icons/lixeira.png"
                          alt=""
                          width={20}
                          height={20}
                          className="pointer-events-none size-5 object-contain"
                          draggable={false}
                        />
                      </button>
                    ) : null}
                    <span className="pointer-events-none absolute bottom-[28%] left-1/2 z-[1] w-[calc(100%-1.5rem)] -translate-x-1/2 px-2 text-center font-amiri-quran text-sm font-medium leading-tight text-white drop-shadow [text-shadow:0_1px_2px_rgba(0,0,0,0.8)]">
                      {r.nome}
                    </span>
                  </div>
                ))}

                {isAdmin ? (
                  <button
                    type="button"
                    onClick={() => setModalOpen(true)}
                    className={`flex aspect-[16/10] w-full flex-col items-center justify-center gap-2 ${BTN_RADIUS} border-2 border-dashed border-gray-400/70 bg-white/50 font-arima text-gray-500 transition hover:border-[#910A21]/50 hover:bg-[#910A21]/5 hover:text-[#910A21] focus-visible:outline focus-visible:ring-2 focus-visible:ring-[#910A21]/40`}
                  >
                    <Plus className="size-12 stroke-[1.25]" aria-hidden />
                    <span className="text-sm font-medium">Novo relatório</span>
                  </button>
                ) : null}
              </div>
            </div>
          ) : (
            <UsersAdminPanel onBack={goHome} />
          )}
        </main>
      ) : null}

      {!fullscreen ? (
        <footer className="shrink-0 pb-4 pt-2">
          <div className="flex justify-center">
            <img
              src="/logos_carteiras/logo_trc_burgundy.png"
              alt="TRC"
              width={40}
              height={17}
              className="h-[17px] w-10 object-contain opacity-90"
            />
          </div>
        </footer>
      ) : null}

      {panel === 'trc' ? (
        <FullScreenReport src={FIXED_EMBEDS.trc.src} onBack={goHome} />
      ) : null}
      {panel === 'ranking' ? (
        <FullScreenReport src={FIXED_EMBEDS.ranking.src} onBack={goHome} />
      ) : null}
      {panel === 'report' && cardReport ? (
        <FullScreenReport src={cardReport.link} onBack={goHome} />
      ) : null}

      {isAdmin ? (
        <AddReportModal
          open={modalOpen}
          onClose={() => setModalOpen(false)}
          onSaved={() => loadReports()}
        />
      ) : null}
    </div>
  )
}
