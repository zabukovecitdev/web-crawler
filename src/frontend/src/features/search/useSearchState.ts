import { useSearchParams } from 'react-router-dom'
import type { SearchParams } from './search-api'

function toParams(next: SearchParams): URLSearchParams {
  const out = new URLSearchParams()
  if (next.q) {
    out.set('q', next.q)
  }
  if (next.host) {
    out.set('host', next.host)
  }
  if (next.page > 1) {
    out.set('page', String(next.page))
  }
  return out
}

/** The URL is the source of truth: ?q=&host=&page= */
export function useSearchState() {
  const [params, setParams] = useSearchParams()
  const state: SearchParams = {
    q: params.get('q') ?? '',
    host: params.get('host') ?? '',
    page: Math.max(1, Number(params.get('page')) || 1),
  }
  const update = (patch: Partial<SearchParams>) => setParams(toParams({ ...state, ...patch }))
  return { state, update }
}
