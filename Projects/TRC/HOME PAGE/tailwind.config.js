import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = path.dirname(fileURLToPath(import.meta.url))

/** Raiz do projeto — usada para resolver pastas de assets e protótipos. */
const root = __dirname

/**
 * Caminhos no `content`: classes Tailwind em app, e eventualmente em /design.
 * Imagens em si ficam em /img e /icons e são referenciadas como URL (/img/...),
 * não precisam ser listadas em `content` para o JIT; mantemos rotas documentadas
 * em `theme.extend` para convenções de background do app.
 */
export default {
  content: [
    path.join(root, 'index.html'),
    path.join(root, 'src/**/*.{js,ts,jsx,tsx}'),
    path.join(root, 'design/**/*.{html,js,jsx,tsx,md}'),
  ],
  theme: {
    extend: {
      fontFamily: {
        arima: ['Arima', 'cursive'],
        arimo: ['Arimo', 'sans-serif'],
      },
      /**
       * Fundos: usar URLs servidas pelo Vite — ver `architecture.md`.
       */
    },
  },
  plugins: [],
}
