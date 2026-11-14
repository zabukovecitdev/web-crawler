const THIN_SPACE = ' '
const DASH = '—'

function isUsableNumber(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value)
}

/** 12482 -> "12 482" (thin space, as in the design). */
export function formatNumber(value: number | null | undefined): string {
  if (!isUsableNumber(value)) {
    return DASH
  }
  return Math.round(value).toString().replace(/\B(?=(\d{3})+(?!\d))/g, THIN_SPACE)
}

export function formatMs(value: number | null | undefined): string {
  return isUsableNumber(value) ? `${formatNumber(value)} ms` : DASH
}

const UNITS = ['B', 'KB', 'MB', 'GB', 'TB']

export function formatBytes(value: number | null | undefined): string {
  if (!isUsableNumber(value)) {
    return DASH
  }
  let size = value
  let unit = 0
  while (size >= 1024 && unit < UNITS.length - 1) {
    size /= 1024
    unit += 1
  }
  const text = unit === 0 ? String(size) : size.toFixed(size < 10 ? 1 : 0)
  return `${text} ${UNITS[unit]}`
}

export function formatPercent(ratio: number): string {
  if (!isUsableNumber(ratio)) {
    return DASH
  }
  return `${(ratio * 100).toFixed(ratio < 0.1 ? 1 : 0)} %`
}

const dateTime = new Intl.DateTimeFormat('en-GB', {
  day: 'numeric',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
})

export function formatDate(iso: string | null | undefined): string {
  if (!iso) {
    return DASH
  }
  const date = new Date(iso)
  return Number.isNaN(date.getTime()) ? DASH : dateTime.format(date)
}

const MINUTE = 60_000
const HOUR = 60 * MINUTE
const DAY = 24 * HOUR

function ago(count: number, unit: string): string {
  return `${count} ${unit}${count === 1 ? '' : 's'} ago`
}

/** "Now", "5 minutes ago", "2 hours ago", else a date. */
export function formatRelative(iso: string | null | undefined, now: number = Date.now()): string {
  if (!iso) {
    return 'Never'
  }
  const time = new Date(iso).getTime()
  if (Number.isNaN(time)) {
    return DASH
  }
  const diff = now - time
  if (diff < MINUTE) {
    return 'Now'
  }
  if (diff < HOUR) {
    return ago(Math.floor(diff / MINUTE), 'minute')
  }
  if (diff < DAY) {
    return ago(Math.floor(diff / HOUR), 'hour')
  }
  return formatDate(iso)
}

/** Elapsed time as HH:MM:SS. */
export function formatDuration(ms: number): string {
  const total = Math.floor(ms / 1000)
  const pad = (n: number) => String(n).padStart(2, '0')
  return `${pad(Math.floor(total / 3600))}:${pad(Math.floor((total % 3600) / 60))}:${pad(total % 60)}`
}
