import { useState } from 'react'

const MODAL_RADIUS = 'rounded-[12.1px]'
const FIELD_RADIUS = 'rounded-[8.4px]'
const ACTION_BTN_RADIUS = 'rounded-[11.55px]'

export default function AddReportModal({ open, onClose, onSaved }) {
  const [nome, setNome] = useState('')
  const [link, setLink] = useState('')
  const [file, setFile] = useState(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')

  if (!open) return null

  function reset() {
    setNome('')
    setLink('')
    setFile(null)
    setError('')
  }

  function handleClose() {
    reset()
    onClose()
  }

  async function handleSubmit(e) {
    e.preventDefault()
    setError('')
    const n = nome.trim()
    const l = link.trim()
    if (!n || !l) {
      setError('Preencha o nome e o link do Power BI.')
      return
    }
    if (!file) {
      setError('Selecione a imagem de fundo do cartão.')
      return
    }
    if (!l.startsWith('https://')) {
      setError('O link deve começar por https://')
      return
    }

    setBusy(true)
    try {
      const dataUrl = await readFileAsDataUrl(file)
      const res = await fetch('/api/reports', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ nome: n, link: l, imageDataUrl: dataUrl }),
      })
      const body = await res.json().catch(() => ({}))
      if (!res.ok) {
        setError(body.error || `Não foi possível gravar (${res.status}).`)
        return
      }
      onSaved?.(body.report)
      reset()
      onClose()
    } catch {
      setError(
        'Falha ao enviar. Em build estático use npm run dev para gravar ficheiros e atualizar o JSON.'
      )
    } finally {
      setBusy(false)
    }
  }

  return (
    <div
      className="fixed inset-0 z-[110] flex items-center justify-center bg-black/40 p-4 backdrop-blur-[2px]"
      role="presentation"
      onClick={(ev) => ev.target === ev.currentTarget && handleClose()}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-labelledby="add-report-title"
        className={`w-full max-w-md border border-[#910A21]/20 bg-white p-6 shadow-xl ${MODAL_RADIUS}`}
        onClick={(e) => e.stopPropagation()}
      >
        <h2
          id="add-report-title"
          className="font-arima text-lg font-semibold text-[#910A21]"
        >
          Novo relatório
        </h2>
        <p className="mt-1 text-xs text-gray-600">
          Os relatórios aparecem na grelha; pode fazer scroll para ver todos. O link do Power
          BI não é mostrado nos cartões.
        </p>

        <form onSubmit={handleSubmit} className="mt-4 flex flex-col gap-3">
          <label className="block text-sm text-gray-700">
            Nome do relatório
            <input
              type="text"
              value={nome}
              onChange={(e) => setNome(e.target.value)}
              className={`mt-1 w-full border border-gray-200 px-3 py-2 text-sm outline-none ring-[#910A21]/30 focus:ring-2 ${FIELD_RADIUS}`}
              autoComplete="off"
            />
          </label>
          <label className="block text-sm text-gray-700">
            Link publicado do Power BI
            <input
              type="url"
              value={link}
              onChange={(e) => setLink(e.target.value)}
              placeholder="https://app.powerbi.com/view?..."
              className={`mt-1 w-full border border-gray-200 px-3 py-2 font-mono text-xs outline-none ring-[#910A21]/30 focus:ring-2 ${FIELD_RADIUS}`}
              autoComplete="off"
            />
          </label>
          <label className="block text-sm text-gray-700">
            Imagem de fundo do cartão
            <input
              type="file"
              accept="image/png,image/jpeg,image/webp"
              onChange={(e) => setFile(e.target.files?.[0] ?? null)}
              className="mt-1 w-full text-sm text-gray-600 file:mr-2 file:rounded-[8.4px] file:border-0 file:bg-[#910A21]/10 file:px-3 file:py-1.5 file:text-sm file:text-[#910A21]"
            />
          </label>

          {error ? (
            <p className="text-sm text-red-600" role="alert">
              {error}
            </p>
          ) : null}

          <div className="mt-2 flex justify-end gap-2">
            <button
              type="button"
              onClick={handleClose}
              className={`${ACTION_BTN_RADIUS} px-4 py-2 text-sm text-gray-600 hover:bg-gray-100`}
            >
              Cancelar
            </button>
            <button
              type="submit"
              disabled={busy}
              className={`${ACTION_BTN_RADIUS} bg-[#910A21] px-4 py-2 text-sm font-medium text-white disabled:opacity-60`}
            >
              {busy ? 'A gravar…' : 'Cadastrar'}
            </button>
          </div>
        </form>
      </div>
    </div>
  )
}

function readFileAsDataUrl(file) {
  return new Promise((resolve, reject) => {
    const r = new FileReader()
    r.onload = () => resolve(String(r.result))
    r.onerror = () => reject(r.error)
    r.readAsDataURL(file)
  })
}
