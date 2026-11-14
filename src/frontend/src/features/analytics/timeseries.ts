import { formatNumber } from '@/lib/format'
import type { TimeseriesPoint } from '@/types/api'
import type { Bucket } from './analytics-api'

export type Column = { label: string; tip: string; requests: number }

const hourLabel = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false })
const dayLabel = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short' })
const dayLong = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })

function labels(iso: string, bucket: Bucket): { label: string; tip: string } {
  const date = new Date(iso)
  if (bucket === 'hour') {
    const hour = hourLabel.format(date)
    return { label: hour, tip: `${dayLabel.format(date)}, ${hour}` }
  }
  return { label: dayLabel.format(date), tip: dayLong.format(date) }
}

export function toColumns(points: TimeseriesPoint[], bucket: Bucket): Column[] {
  return [...points]
    .sort((a, b) => a.bucket.localeCompare(b.bucket))
    .map((p) => ({ ...labels(p.bucket, bucket), requests: p.requests }))
}

export function peakIndex(columns: Column[]): number {
  let best = -1
  columns.forEach((c, i) => {
    if (c.requests > 0 && (best < 0 || c.requests > columns[best].requests)) {
      best = i
    }
  })
  return best
}

export function seriesKicker(columns: Column[], bucket: Bucket, phrase: string): string {
  const peak = peakIndex(columns)
  if (peak < 0) {
    return phrase
  }
  const unit = bucket === 'hour' ? 'at' : 'on'
  return `${phrase} · peak ${formatNumber(columns[peak].requests)} ${unit} ${columns[peak].label}`
}

export function seriesSummary(columns: Column[], bucket: Bucket): string {
  const total = columns.reduce((sum, c) => sum + c.requests, 0)
  const peak = peakIndex(columns)
  const per = bucket === 'hour' ? 'hour' : 'day'
  const head = `Requests per ${per}, ${formatNumber(total)} in total over ${columns.length} ${per}s.`
  return peak < 0 ? head : `${head} Peak ${formatNumber(columns[peak].requests)} at ${columns[peak].tip}.`
}
