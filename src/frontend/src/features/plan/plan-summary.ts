import type { JsVerdict, RenderMode } from '@/types/api'

const MINUTE = 60

export function formatNumber(value: number): string {
  return value.toLocaleString('en-US')
}

export function formatEstimate(seconds: number): string {
  if (seconds <= 0) {
    return '0 min'
  }
  const minutes = Math.round(seconds / MINUTE)
  if (minutes < 1) {
    return 'under 1 min'
  }
  if (minutes < 60) {
    return `${minutes} min`
  }
  const rest = minutes % 60
  return rest > 0 ? `${Math.floor(minutes / 60)} h ${rest} min` : `${minutes / 60} h`
}

export function usesBrowser(mode: RenderMode, verdict: JsVerdict): boolean {
  return mode === 'Always' || (mode === 'Auto' && verdict !== 'NotNeeded')
}

export function secondsPerPage(delaySeconds: number | null, inBrowser: boolean): number {
  const delay = delaySeconds ?? 1
  return inBrowser ? Math.max(3, delay) : Math.max(1, delay)
}

export type SummaryInput = {
  ticked: number
  total: number
  hasSitemap: boolean
  seconds: number
  inBrowser: boolean
  linkedExtra: boolean
  review: boolean
}

export type Summary = { value: string; label: string; sub: string }

export function summarize(input: SummaryInput): Summary {
  const value = formatNumber(input.ticked)
  if (!input.hasSitemap) {
    const rest = input.review ? 'The crawl pauses after 100 pages so you can review the map.' : 'It runs until no new pages turn up.'
    return { value, label: 'pages known so far', sub: `No sitemap: more are found while crawling. ${rest}` }
  }
  const how = input.inBrowser ? 'browser' : 'HTTP'
  const extra = input.linkedExtra ? ' · plus linked pages that are not in the sitemap' : ''
  return {
    value,
    label: input.ticked === 1 ? 'page ticked' : 'pages ticked',
    sub: `of ${formatNumber(input.total)} in the sitemap · about ${formatEstimate(input.seconds)} · ${how}${extra}`,
  }
}
