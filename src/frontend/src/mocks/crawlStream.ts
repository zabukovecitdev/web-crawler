import type { CrawlEvent } from '@/types/api'
import { completedEvent, startedEvent } from './data/crawl'

export type MockCrawlStream = {
  /** Registers a listener; returns the unsubscribe function. The first listener starts the timer. */
  subscribe: (listener: (event: CrawlEvent) => void) => () => void
  /** Stops the timer and drops all listeners. */
  close: () => void
}

/**
 * Stand-in for the SignalR `CrawlEvent` stream (hub /hubs/crawl-job, JoinJob(jobId)).
 * Every `intervalMs` it emits FetchStarted, then FetchCompleted a bit later, cycling the
 * sample rows from the Live design. Same event shapes as src/types/api.ts.
 */
export function createMockCrawlStream(jobId: number, intervalMs = 1200): MockCrawlStream {
  const listeners = new Set<(event: CrawlEvent) => void>()
  let timer: ReturnType<typeof setInterval> | null = null
  let tick = 0

  const emit = (event: CrawlEvent) => listeners.forEach((listener) => listener(event))

  const step = () => {
    const i = tick
    tick += 1
    emit(startedEvent(jobId, i))
    setTimeout(() => emit(completedEvent(jobId, i)), intervalMs / 2)
  }

  const close = () => {
    if (timer) {
      clearInterval(timer)
      timer = null
    }
    listeners.clear()
  }

  const subscribe = (listener: (event: CrawlEvent) => void) => {
    listeners.add(listener)
    timer ??= setInterval(step, intervalMs)
    return () => {
      listeners.delete(listener)
      if (listeners.size === 0) {
        close()
      }
    }
  }

  return { subscribe, close }
}
