import { formatNumber, formatPercent } from '@/lib/format'
import type { StatusClassCounts } from '@/types/api'

export type StatusRow = {
  key: keyof StatusClassCounts
  label: string
  count: number
  share: string
  /** CSS colour from the Zabukovec variables. */
  color: string
}

const CLASSES: { key: keyof StatusClassCounts; label: string; color: string }[] = [
  { key: 'ok', label: '2xx OK', color: 'rgb(var(--led))' },
  { key: 'redirect', label: '3xx Redirect', color: 'rgb(var(--ink-faint))' },
  { key: 'clientError', label: '4xx Client error', color: 'rgb(var(--amber-ink))' },
  { key: 'serverError', label: '5xx Server error', color: 'rgb(var(--primary))' },
]

export function statusTotal(counts: StatusClassCounts): number {
  return counts.ok + counts.redirect + counts.clientError + counts.serverError
}

export function statusRows(counts: StatusClassCounts): StatusRow[] {
  const total = statusTotal(counts)
  return CLASSES.map((c) => ({
    ...c,
    count: counts[c.key],
    share: formatPercent(total ? counts[c.key] / total : 0),
  }))
}

export function statusSummary(rows: StatusRow[]): string {
  return rows.map((r) => `${formatNumber(r.count)} ${r.label}`).join(', ')
}
