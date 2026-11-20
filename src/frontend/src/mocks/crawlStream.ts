import type { ConnectionState } from '@/features/crawl/crawl-hub'
import type { CrawlEvent } from '@/types/api'
import { eventById, recordEmitted, streamEventId } from './data/crawl'

export type MockCrawlStream = {
  subscribe: (listener: (event: CrawlEvent) => void) => () => void
  onState: (listener: (state: ConnectionState) => void) => void
  drop: (ms: number) => void
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

function send(core: Core, jobId: number, id: number): void {
  recordEmitted(id)
  emit(core, eventById(jobId, id))
}

function step(core: Core, jobId: number, intervalMs: number): void {
  const i = core.tick
  core.tick += 1
  send(core, jobId, streamEventId(i, false))
  setTimeout(() => send(core, jobId, streamEventId(i, true)), intervalMs / 2)
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
