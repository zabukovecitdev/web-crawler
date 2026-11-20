import type { ContentStatus, CrawlEvent, CrawlJobDto, CrawlProgress, JobLinkSummary } from '@/types/api'
import { ago, HOUR, MINUTE } from '../util'

import { LIVE_JOB_ID } from './sites'

export const NO_LINKS: JobLinkSummary = { total: 0, queued: 0, fetching: 0, retrying: 0, parsing: 0, indexing: 0, indexed: 0, notIndexed: 0, skipped: 0, failed: 0, problems: [] }

function summary(parts: Partial<JobLinkSummary>): JobLinkSummary {
  const merged = { ...NO_LINKS, ...parts }
  const total = merged.queued + merged.fetching + merged.retrying + merged.parsing + merged.indexing + merged.indexed + merged.notIndexed + merged.skipped + merged.failed
  return { ...merged, total }
}

const LIVE_LINKS = summary({
  queued: 3420, fetching: 1, retrying: 2, parsing: 24, indexing: 11, indexed: 1150, notIndexed: 40, skipped: 70, failed: 7,
  problems: [{ reason: 'ServerError', label: 'Server error (5xx)', count: 5 }, { reason: 'Timeout', label: 'Timed out', count: 4 }],
})
const DONE_LINKS = summary({ indexed: 3010, notIndexed: 64, skipped: 118, failed: 12, problems: [{ reason: 'NotFound', label: 'Not found (404)', count: 12 }] })
const SMALL_DONE_LINKS = summary({ indexed: 412, notIndexed: 6, skipped: 21 })
const CANCELLED_LINKS = summary({ queued: 640, indexed: 220, skipped: 14, failed: 2, problems: [{ reason: 'Timeout', label: 'Timed out', count: 2 }] })
const PAUSED_LINKS = summary({ queued: 300, parsing: 12, indexed: 180, notIndexed: 3, skipped: 9 })

export const JOBS: CrawlJobDto[] = [
  { id: LIVE_JOB_ID, status: 'Running', host: 'docs.example.com', seedUrls: ['https://docs.example.com/'], maxDepth: 5, maxUrls: 5000, useJsRendering: false, createdAt: ago(25 * MINUTE), startedAt: ago(24 * MINUTE + 12_000), completedAt: null, links: LIVE_LINKS },
  { id: 41, status: 'Completed', host: 'example.org', seedUrls: ['https://example.org/'], maxDepth: 5, maxUrls: 5000, useJsRendering: false, createdAt: ago(4 * HOUR), startedAt: ago(4 * HOUR), completedAt: ago(2 * HOUR), links: DONE_LINKS },
  { id: 40, status: 'Completed', host: 'blog.example.net', seedUrls: ['https://blog.example.net/'], maxDepth: 4, maxUrls: 3000, useJsRendering: false, createdAt: ago(31 * HOUR), startedAt: ago(31 * HOUR), completedAt: ago(30 * HOUR), links: SMALL_DONE_LINKS },
  { id: 39, status: 'Cancelled', host: 'shop.example.com', seedUrls: ['https://shop.example.com/'], maxDepth: 3, maxUrls: 2000, useJsRendering: true, createdAt: ago(77 * HOUR), startedAt: ago(77 * HOUR), completedAt: ago(76 * HOUR), links: CANCELLED_LINKS },
  { id: 38, status: 'Paused', host: 'wiki.example.edu', seedUrls: ['https://wiki.example.edu/'], maxDepth: 5, maxUrls: 1000, useJsRendering: false, createdAt: ago(121 * HOUR), startedAt: ago(121 * HOUR), completedAt: null, links: PAUSED_LINKS },
]

const RUNNING = {
  crawled: 1284,
  queued: 3528,
  inFlight: 1,
  failed: 9,
  currentDepth: 3,
  pagesPerMinute: 53,
  statusCounts: { ok: 1180, redirect: 74, clientError: 21, serverError: 9 },
}

const FINISHED = {
  crawled: 3204,
  queued: 0,
  inFlight: 0,
  failed: 12,
  currentDepth: null,
  pagesPerMinute: null,
  statusCounts: { ok: 3100, redirect: 92, clientError: 8, serverError: 4 },
}

export function progressOf(job: CrawlJobDto): CrawlProgress {
  const live = job.status === 'Running'
  return {
    ...(live ? RUNNING : FINISHED),
    jobId: job.id,
    status: job.status,
    maxUrls: job.maxUrls,
    currentUrl: live ? `https://${job.host}/reference/storage/object-names` : null,
    crawlDelayMs: 1000,
    delaySource: 'robots.txt',
    nextRequestAt: live ? ago(-800) : null,
    avgResponseMs: 184,
    maxResponseMs: 5004,
    startedAt: job.startedAt,
  }
}

type Row = { path: string; statusCode: number; ms: number; length: number; content: ContentStatus; type: string }

const ROWS: Row[] = [
  { path: '/reference/storage', statusCode: 200, ms: 166, length: 41700, content: 'Stored', type: 'text/html' },
  { path: '/reference/search-index', statusCode: 200, ms: 203, length: 58300, content: 'Stored', type: 'text/html' },
  { path: '/downloads/manual.pdf', statusCode: 200, ms: 912, length: 2400000, content: 'Skipped', type: 'application/pdf' },
  { path: '/guide/installation', statusCode: 200, ms: 172, length: 48200, content: 'Stored', type: 'text/html' },
  { path: '/upgrade', statusCode: 301, ms: 61, length: 300, content: 'NotStored', type: 'text/html' },
  { path: '/guide/legacy-setup', statusCode: 404, ms: 88, length: 2100, content: 'NotStored', type: 'text/html' },
  { path: '/api/playground', statusCode: 503, ms: 5004, length: 900, content: 'Failed', type: 'text/html' },
  { path: '/api/crawl-jobs', statusCode: 200, ms: 201, length: 74000, content: 'Stored', type: 'text/html' },
]

export function startedEvent(jobId: number, i: number): CrawlEvent {
  const row = ROWS[i % ROWS.length]
  return { crawlJobId: jobId, eventType: 'FetchStarted', at: new Date().toISOString(), url: `https://docs.example.com${row.path}`, depth: 2 }
}

export function completedEvent(jobId: number, i: number): CrawlEvent {
  const row = ROWS[i % ROWS.length]
  return {
    crawlJobId: jobId,
    eventType: 'FetchCompleted',
    at: new Date().toISOString(),
    pageId: 3,
    host: 'docs.example.com',
    url: `https://docs.example.com${row.path}`,
    depth: 2,
    statusCode: row.statusCode,
    responseTimeMs: row.ms,
    contentType: row.type,
    contentLength: row.length,
    contentStatus: row.content,
    delayMs: 1000,
    outcome: row.statusCode < 400 ? 'Success' : 'Failed',
  }
}

export function recentEvents(jobId: number, after?: number, limit = 100): (CrawlEvent & { id: number })[] {
  const events = Array.from({ length: ROWS.length }, (_, i) => ({
    ...completedEvent(jobId, i),
    at: ago((ROWS.length - i) * 1500),
    id: i + 1,
  }))
  return after === undefined ? events.slice(-limit) : events.filter((event) => event.id > after).slice(0, limit)
}
