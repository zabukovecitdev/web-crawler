import type { ContentStatus } from '@/types/api'
import type { FetchCompletedEvent } from './crawl-counters'

export const LOG_LIMIT = 200

export type LogRow = {
  key: string
  at: string
  url: string
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
    statusCode: event.statusCode,
    responseTimeMs: event.responseTimeMs,
    contentLength: event.contentLength,
    contentStatus: event.contentStatus,
  }
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
