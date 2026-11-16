import type { ContentStatus } from '@/types/api'
import type { FetchCompletedEvent } from './crawl-counters'

export const LOG_LIMIT = 200

export type LogRow = {
  key: string
  at: string
  url: string
  /** The page screen of this URL, or null for events recorded before they carried the page id. */
  pagePath: string | null
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

function hostOf(url: string): string | null {
  try {
    return new URL(url).host
  } catch {
    return null
  }
}

/** The dashboard route of the page an event is about: /sites/{host}/pages/{id}. Null when the event has no page id. */
export function pagePathOf(event: Pick<FetchCompletedEvent, 'pageId' | 'host' | 'url'>): string | null {
  const host = event.host ?? hostOf(event.url)
  return event.pageId && host ? `/sites/${encodeURIComponent(host)}/pages/${event.pageId}` : null
}

/** Newest first, no duplicates, at most LOG_LIMIT rows. */
export function mergeRows(existing: LogRow[], incoming: LogRow[]): LogRow[] {
  const byKey = new Map<string, LogRow>()
  for (const row of [...incoming, ...existing]) {
    byKey.set(row.key, byKey.get(row.key) ?? row)
  }
  return [...byKey.values()].sort((a, b) => b.at.localeCompare(a.at)).slice(0, LOG_LIMIT)
}

/** "/reference/storage" for a full URL; the input unchanged when it cannot be parsed. */
export function pathOf(url: string): string {
  try {
    const parsed = new URL(url)
    return `${parsed.pathname}${parsed.search}`
  } catch {
    return url
  }
}
