/**
 * URLs dos relatórios fixos (TRC / Ranking). Não expor estes valores na UI —
 * usar apenas chaves internas e iframes.
 */
export const FIXED_EMBEDS = {
  trc: {
    key: 'trc',
    title: 'TRC',
    src:
      'https://app.powerbi.com/view?r=eyJrIjoiMWRmMzQyYTEtZTQ4Mi00YmEzLTkzYjYtY2I4ODY1ZTQ2MjBmIiwidCI6ImNhNjRhMDU3LTEyYjctNDRkOS1iYTU4LTNiNjNhOTVlNzNhYiJ9&disablecdnExpiration=1764012134',
  },
  ranking: {
    key: 'ranking',
    title: 'Ranking',
    src:
      'https://app.powerbi.com/view?r=eyJrIjoiNTdmNDRhMDctNzU0Ny00MTdiLWI0NTYtYTUxMTA3ZmEzODQ4IiwidCI6ImNhNjRhMDU3LTEyYjctNDRkOS1iYTU4LTNiNjNhOTVlNzNhYiJ9',
  },
}
