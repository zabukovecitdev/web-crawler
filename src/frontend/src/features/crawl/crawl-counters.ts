import type { CrawlEvent, CrawlJobStatus, CrawlProgress } from '@/types/api'

export type FetchCompletedEvent = Extract<CrawlEvent, { eventType: 'FetchCompleted' }>

type StatusCounts = CrawlProgress['statusCounts']

function bumpCounts(counts: StatusCounts, statusCode: number): StatusCounts {
  if (statusCode >= 500) {
    return { ...counts, serverError: counts.serverError + 1 }
  }
  if (statusCode >= 400) {
    return { ...counts, clientError: counts.clientError + 1 }
  }
  if (statusCode >= 300) {
    return { ...counts, redirect: counts.redirect + 1 }
  }
  return { ...counts, ok: counts.ok + 1 }
}

function isFailure(event: FetchCompletedEvent): boolean {
  return event.statusCode >= 400 || event.contentStatus === 'Failed'
}

function slowest(current: number | null, next: number | null): number | null {
  return Math.max(current ?? 0, next ?? 0) || null
}

/** Moves the counters on by one finished request. The next snapshot replaces them. */
export function applyCompleted(progress: CrawlProgress, event: FetchCompletedEvent): CrawlProgress {
  return {
    ...progress,
    crawled: progress.crawled + 1,
    queued: Math.max(0, progress.queued - 1),
    inFlight: 0,
    failed: progress.failed + (isFailure(event) ? 1 : 0),
    statusCounts: bumpCounts(progress.statusCounts, event.statusCode),
    maxResponseMs: slowest(progress.maxResponseMs, event.responseTimeMs),
    crawlDelayMs: event.delayMs,
    nextRequestAt: new Date(new Date(event.at).getTime() + event.delayMs).toISOString(),
  }
}

const STATUS_BY_EVENT: Partial<Record<CrawlEvent['eventType'], CrawlJobStatus>> = {
  JobStarted: 'Running',
  JobPaused: 'Paused',
  JobCancelled: 'Cancelled',
  JobCompleted: 'Completed',
}

export function applyJobEvent(progress: CrawlProgress, eventType: CrawlEvent['eventType']): CrawlProgress {
  const status = STATUS_BY_EVENT[eventType]
  return status ? { ...progress, status } : progress
}
