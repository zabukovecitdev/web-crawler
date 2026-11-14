import type { HostAnalytics } from '@/types/api'

export type SortKey = Exclude<keyof HostAnalytics, never>
export type Sort = { key: SortKey; dir: 'asc' | 'desc' }

export const DEFAULT_SORT: Sort = { key: 'pages', dir: 'desc' }

function compare(a: HostAnalytics, b: HostAnalytics, key: SortKey): number {
  const x = a[key]
  const y = b[key]
  if (typeof x === 'string' && typeof y === 'string') {
    return x.localeCompare(y)
  }
  // Missing values (null) always sort as the smallest.
  return (Number(x ?? -1)) - (Number(y ?? -1))
}

export function sortHosts(hosts: HostAnalytics[], sort: Sort): HostAnalytics[] {
  const sign = sort.dir === 'asc' ? 1 : -1
  return [...hosts].sort((a, b) => sign * compare(a, b, sort.key))
}

/** Clicking the active column flips direction; a new column starts descending (host: ascending). */
export function nextSort(current: Sort, key: SortKey): Sort {
  if (current.key === key) {
    return { key, dir: current.dir === 'asc' ? 'desc' : 'asc' }
  }
  return { key, dir: key === 'host' ? 'asc' : 'desc' }
}
