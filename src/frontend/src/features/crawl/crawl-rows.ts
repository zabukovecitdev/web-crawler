import type { ContentStatus } from '@/types/api'
import type { FetchCompletedEvent } from './crawl-counters'

export const LOG_LIMIT = 200

export type LogRow = {
  key: string
  at: string
  url: string
  pagePath: string
  statusCode: number
  responseTimeMs: number | null
  contentLength: number | null
  contentStatus: ContentStatus
}

export function rowOf(event: FetchCompletedEvent): LogRow {
  return {
    key: `${event.at}|${event.url}`,
    at: event.at,
    url: event.url,
    pagePath: pagePathOf(event),
    statusCode: event.statusCode,
    responseTimeMs: event.responseTimeMs,
    contentLength: event.contentLength,
    contentStatus: event.contentStatus,
  }
}

export function pagePathOf(event: Pick<FetchCompletedEvent, 'pageId' | 'host'>): string {
  return `/sites/${encodeURIComponent(event.host)}/pages/${event.pageId}`
}

export function mergeRows(existing: LogRow[], incoming: LogRow[]): LogRow[] {
  const byKey = new Map<string, LogRow>()
  for (const row of [...incoming, ...existing]) {
    byKey.set(row.key, byKey.get(row.key) ?? row)
  }
  return [...byKey.values()].sort((a, b) => b.at.localeCompare(a.at)).slice(0, LOG_LIMIT)
}

export function pathOf(url: string): string {
  try {
    const parsed = new URL(url)
    return `${parsed.pathname}${parsed.search}`
  } catch {
    return url
  }
}
