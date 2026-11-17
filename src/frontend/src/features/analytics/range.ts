import { useCallback } from 'react'
import { useSearchParams } from 'react-router-dom'
import type { AnalyticsRange } from '@/types/api'

export const RANGES: { value: AnalyticsRange; label: string; phrase: string }[] = [
  { value: '24h', label: '24 hours', phrase: 'Last 24 hours' },
  { value: '7d', label: '7 days', phrase: 'Last 7 days' },
  { value: '30d', label: '30 days', phrase: 'Last 30 days' },
  { value: 'all', label: 'All time', phrase: 'All time' },
]

const DEFAULT_RANGE: AnalyticsRange = 'all'

export function parseRange(value: string | null): AnalyticsRange {
  return RANGES.find((r) => r.value === value)?.value ?? DEFAULT_RANGE
}

export function rangePhrase(range: AnalyticsRange): string {
  return RANGES.find((r) => r.value === range)?.phrase ?? ''
}

export type AnalyticsView = 'overview' | 'graph'

export function parseView(value: string | null): AnalyticsView {
  return value === 'graph' ? 'graph' : 'overview'
}

export type Filters = {
  view: AnalyticsView
  setView: (view: AnalyticsView) => void
  range: AnalyticsRange
  host: string | undefined
  setRange: (range: AnalyticsRange) => void
  setHost: (host: string | undefined) => void
}

/** Range and host live in the URL (?range=&host=) so views can be shared. */
export function useFilters(): Filters {
  const [params, setParams] = useSearchParams()
  const update = useCallback(
    (key: string, value: string | undefined) => {
      setParams(
        (prev) => {
          const next = new URLSearchParams(prev)
          if (value) {
            next.set(key, value)
          } else {
            next.delete(key)
          }
          return next
        },
        { replace: true },
      )
    },
    [setParams],
  )
  return {
    view: parseView(params.get('view')),
    setView: (view) => update('view', view === 'overview' ? undefined : view),
    range: parseRange(params.get('range')),
    host: params.get('host') ?? undefined,
    setRange: (range) => update('range', range),
    setHost: (host) => update('host', host),
  }
}
