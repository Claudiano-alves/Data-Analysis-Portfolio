import { ArrowLeft } from 'lucide-react'

/** Ecrã inteiro: só iframe + botão flutuante para voltar (sem barra/título). */
export default function FullScreenReport({ src, onBack }) {
  return (
    <div className="fixed inset-0 z-[100] bg-neutral-900">
      <iframe
        title="Relatório Power BI"
        src={src}
        className="h-full w-full border-0 bg-white"
        allowFullScreen
        sandbox="allow-scripts allow-same-origin allow-popups allow-forms allow-popups-to-escape-sandbox"
      />
      <button
        type="button"
        onClick={onBack}
        aria-label="Voltar à home"
        className="absolute left-4 top-4 z-[101] flex size-11 items-center justify-center rounded-[11.55px] border border-[#910A21] bg-[#910A21]/95 text-white shadow-md transition hover:bg-[#910A21] focus-visible:outline focus-visible:ring-2 focus-visible:ring-white/70"
      >
        <ArrowLeft className="size-5" aria-hidden />
      </button>
    </div>
  )
}
