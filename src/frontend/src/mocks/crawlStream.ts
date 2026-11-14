import type { ConnectionState } from '@/features/crawl/crawl-hub'
import type { CrawlEvent } from '@/types/api'
import { completedEvent, startedEvent } from './data/crawl'

export type MockCrawlStream = {
  /** Registers a listener; returns the unsubscribe function. The first listener starts the timer. */
  subscribe: (listener: (event: CrawlEvent) => void) => () => void
  /** Registers the connection-state listener (what the SignalR client would report). */
  onState: (listener: (state: ConnectionState) => void) => void
  /** Simulates a dropped connection: no events for `ms`, 'reconnecting' meanwhile, then 'live' again. */
  drop: (ms: number) => void
  /** Stops the timer and drops all listeners. */
  close: () => void
}

type Core = {
  listeners: Set<(event: CrawlEvent) => void>
  timer: ReturnType<typeof setInterval> | null
  tick: number
  dropped: boolean
  onState: (state: ConnectionState) => void
}

function emit(core: Core, event: CrawlEvent): void {
  if (!core.dropped) {
    core.listeners.forEach((listener) => listener(event))
  }
}

function step(core: Core, jobId: number, intervalMs: number): void {
  const i = core.tick
  core.tick += 1
  emit(core, startedEvent(jobId, i))
  setTimeout(() => emit(core, completedEvent(jobId, i)), intervalMs / 2)
}

function close(core: Core): void {
  if (core.timer) {
    clearInterval(core.timer)
    core.timer = null
  }
  core.listeners.clear()
}

function drop(core: Core, ms: number): void {
  core.dropped = true
  core.onState('reconnecting')
  setTimeout(() => {
    core.dropped = false
    core.onState('live')
  }, ms)
}

function subscribe(core: Core, listener: (event: CrawlEvent) => void, start: () => void, ms: number): () => void {
  core.listeners.add(listener)
  core.timer ??= setInterval(start, ms)
  return () => {
    core.listeners.delete(listener)
    if (core.listeners.size === 0) {
      close(core)
    }
  }
}

/**
 * Stand-in for the SignalR `CrawlEvent` stream (hub /hubs/crawl-job, JoinJob(jobId)).
 * Every `intervalMs` it emits FetchStarted, then FetchCompleted a bit later, cycling the
 * sample rows from the Live design. Same event shapes as src/types/api.ts. For checking the
 * resync by hand, `window.__crawlMock.drop(8000)` simulates an 8 s connection drop.
 */
export function createMockCrawlStream(jobId: number, intervalMs = 1200): MockCrawlStream {
  const core: Core = { listeners: new Set(), timer: null, tick: 0, dropped: false, onState: () => undefined }
  const stream: MockCrawlStream = {
    subscribe: (listener) => subscribe(core, listener, () => step(core, jobId, intervalMs), intervalMs),
    onState: (listener) => {
      core.onState = listener
    },
    drop: (ms) => drop(core, ms),
    close: () => close(core),
  }
  Object.assign(window, { __crawlMock: stream })
  return stream
}
