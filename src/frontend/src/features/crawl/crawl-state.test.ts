import { describe, expect, it } from 'vitest'
import type { CrawlProgress } from '@/types/api'
import type { LiveEvent, StoredEvent } from './crawl-api'
import { crawlReducer, initialCrawlState, type CrawlState } from './crawl-state'

const PROGRESS = {
  crawlJobId: 1,
  status: 'Running',
  pagesFetched: 10,
  statusCounts: { ok: 10, redirect: 0, clientError: 0, serverError: 0 },
  avgResponseMs: 100,
  maxResponseMs: 200,
} as unknown as CrawlProgress

function completed(id: number | undefined, url = `https://a.test/${id}`): LiveEvent {
  return {
    id,
    crawlJobId: 1,
    eventType: 'FetchCompleted',
    at: `2026-01-01T00:00:${String(id ?? 0).padStart(2, '0')}Z`,
    url,
    depth: 1,
    statusCode: 200,
    responseTimeMs: 100,
    contentType: 'text/html',
    contentLength: 10,
    contentStatus: 'Stored',
    delayMs: 0,
    outcome: 'ok',
  } as LiveEvent
}

const stored = (id: number) => completed(id) as StoredEvent
const base: CrawlState = { ...initialCrawlState, progress: PROGRESS }
const okCount = (s: CrawlState) => s.progress?.statusCounts.ok

it('counts a live event once and ignores the same id in history', () => {
  const live = crawlReducer(base, { type: 'live', event: completed(5) })
  const after = crawlReducer(live, { type: 'history', events: [stored(5)] })
  expect(after.log).toHaveLength(1)
  expect(okCount(after)).toBe(11)
  expect(after.lastEventId).toBe(5)
})

it('ignores a live event whose id history already applied', () => {
  const history = crawlReducer(base, { type: 'history', events: [stored(5)] })
  const after = crawlReducer(history, { type: 'live', event: completed(5) })
  expect(after).toBe(history)
  expect(okCount(after)).toBe(10)
})

it('always applies a live event without an id', () => {
  const once = crawlReducer(base, { type: 'live', event: completed(undefined, 'https://a.test/x') })
  const twice = crawlReducer(once, { type: 'live', event: completed(undefined, 'https://a.test/y') })
  expect(okCount(twice)).toBe(12)
  expect(twice.seen).toEqual([])
})

it('keeps history out of the counters and moves the cursor to the largest id', () => {
  const after = crawlReducer(base, { type: 'history', events: [stored(2), stored(9), stored(4)] })
  expect(okCount(after)).toBe(10)
  expect(after.lastEventId).toBe(9)
})

it('remembers at most 500 ids', () => {
  const events = Array.from({ length: 600 }, (_, i) => stored(i + 1))
  const after = crawlReducer(base, { type: 'history', events })
  expect(after.seen).toHaveLength(500)
  expect(after.seen[0]).toBe(101)
})

describe('snapshot', () => {
  it('replaces the counters the live events moved', () => {
    const live = crawlReducer(base, { type: 'live', event: completed(5) })
    const after = crawlReducer(live, { type: 'snapshot', progress: PROGRESS })
    expect(okCount(after)).toBe(10)
  })
})
