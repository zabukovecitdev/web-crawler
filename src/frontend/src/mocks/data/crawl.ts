import type { ContentStatus, CrawlEvent, CrawlJobDto, CrawlProgress } from '@/types/api'
import { ago, HOUR, MINUTE } from '../util'

import { LIVE_JOB_ID } from './sites'

export const JOBS: CrawlJobDto[] = [
  { id: LIVE_JOB_ID, status: 'Running', host: 'docs.example.com', seedUrls: ['https://docs.example.com/'], maxDepth: 5, maxUrls: 5000, useJsRendering: false, respectRobots: true, createdAt: ago(25 * MINUTE), startedAt: ago(24 * MINUTE + 12_000), completedAt: null },
  { id: 41, status: 'Completed', host: 'example.org', seedUrls: ['https://example.org/'], maxDepth: 5, maxUrls: 5000, useJsRendering: false, respectRobots: true, createdAt: ago(4 * HOUR), startedAt: ago(4 * HOUR), completedAt: ago(2 * HOUR) },
  { id: 40, status: 'Completed', host: 'blog.example.net', seedUrls: ['https://blog.example.net/'], maxDepth: 4, maxUrls: 3000, useJsRendering: false, respectRobots: true, createdAt: ago(31 * HOUR), startedAt: ago(31 * HOUR), completedAt: ago(30 * HOUR) },
  { id: 39, status: 'Cancelled', host: 'shop.example.com', seedUrls: ['https://shop.example.com/'], maxDepth: 3, maxUrls: 2000, useJsRendering: true, respectRobots: true, createdAt: ago(77 * HOUR), startedAt: ago(77 * HOUR), completedAt: ago(76 * HOUR) },
  { id: 38, status: 'Paused', host: 'wiki.example.edu', seedUrls: ['https://wiki.example.edu/'], maxDepth: 5, maxUrls: 1000, useJsRendering: false, respectRobots: true, createdAt: ago(121 * HOUR), startedAt: ago(121 * HOUR), completedAt: null },
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

// Rows from docs/design/Live.dc.html.
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

/** Past requests, oldest id first; the newest one finished 1.5 s ago. */
export function recentEvents(jobId: number, after = 0): (CrawlEvent & { id: number })[] {
  const events = Array.from({ length: ROWS.length }, (_, i) => ({
    ...completedEvent(jobId, i),
    at: ago((ROWS.length - i) * 1500),
    id: i + 1,
  }))
  return events.filter((event) => event.id > after)
}
