import { formatNumber, formatPercent } from '@/lib/format'
import type { HostAnalytics } from '@/types/api'
import type { HostBar } from './HostBarRow'

/** Rounds up to a tidy axis end (2 400, 10 000 ...). */
export function niceMax(value: number): number {
  if (value <= 0) {
    return 1
  }
  const step = 10 ** Math.max(Math.floor(Math.log10(value)) - 1, 0)
  return Math.ceil(value / step) * step
}

const bySlowest = (a: HostAnalytics, b: HostAnalytics) => (b.p50ResponseMs ?? -1) - (a.p50ResponseMs ?? -1)

/** Median bar + p95 marker, slowest median first; the slowest host is the signal bar. */
export function durationBars(hosts: HostAnalytics[]): { bars: HostBar[]; max: number } {
  const sorted = hosts.filter((h) => h.p50ResponseMs !== null).sort(bySlowest)
  const max = niceMax(Math.max(...sorted.map((h) => h.p95ResponseMs ?? h.p50ResponseMs ?? 0)))
  const bars = sorted.map((h, i) => ({
    host: h.host,
    value: h.p50ResponseMs ?? 0,
    max,
    marker: h.p95ResponseMs,
    highlight: i === 0,
    text: `${formatNumber(h.p50ResponseMs)} ms · ${formatNumber(h.p95ResponseMs)}`,
    summary: `${h.host}: median ${formatNumber(h.p50ResponseMs)} ms, p95 ${formatNumber(h.p95ResponseMs)} ms`,
  }))
  return { bars, max }
}

/** Pages per host with their share of all pages, largest first. */
export function pageBars(hosts: HostAnalytics[]): { bars: HostBar[]; total: number } {
  const sorted = [...hosts].sort((a, b) => b.pages - a.pages)
  const total = sorted.reduce((sum, h) => sum + h.pages, 0)
  const max = Math.max(...sorted.map((h) => h.pages), 1)
  const bars = sorted.map((h) => {
    const share = total ? formatPercent(h.pages / total) : formatPercent(0)
    return {
      host: h.host,
      value: h.pages,
      max,
      text: `${formatNumber(h.pages)} · ${share}`,
      summary: `${h.host}: ${formatNumber(h.pages)} pages, ${share} of all pages`,
    }
  })
  return { bars, total }
}
