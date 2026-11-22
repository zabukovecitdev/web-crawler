import type { ContentStatus, CrawlJobStatus, CrawlProgress, PauseReason } from '@/types/api'
import { formatNumber } from '@/lib/format'
import type { Tone } from '@/components/shared/status'

const DASH = '—'

export function formatSeconds(ms: number | null): string {
  return ms === null ? DASH : `${(Math.max(0, ms) / 1000).toFixed(1)} s`
}

export function formatElapsed(ms: number): string {
  const total = Math.max(0, Math.floor(ms / 1000))
  const minutes = Math.floor(total / 60)
  const hours = Math.floor(minutes / 60)
  if (hours > 0) {
    return `${hours} h ${minutes % 60} min`
  }
  return `${minutes} min ${total % 60} s`
}

export function formatClock(iso: string): string {
  return new Date(iso).toLocaleTimeString('en-GB', { hour12: false })
}

const CONTENT_WORDS: Record<ContentStatus, string> = {
  Stored: 'Stored',
  Skipped: 'Skipped',
  NotStored: 'Not stored',
  Failed: 'Failed',
}

export function contentWord(status: ContentStatus): string {
  return CONTENT_WORDS[status]
}

const DELAY_SOURCE_WORDS: Record<CrawlProgress['delaySource'], string> = {
  default: 'Default delay',
  'robots.txt': 'robots.txt Crawl-delay',
  adaptive: 'Adaptive, the site is slow or failing',
}

export function delaySourceWord(source: CrawlProgress['delaySource']): string {
  return DELAY_SOURCE_WORDS[source]
}

export type JobLook = { tone: Tone; word: string; live: boolean }

const JOB_LOOKS: Record<CrawlJobStatus, JobLook> = {
  Draft: { tone: 'neutral', word: 'Draft', live: false },
  Pending: { tone: 'neutral', word: 'Waiting', live: false },
  Running: { tone: 'success', word: 'Crawling', live: true },
  Paused: { tone: 'warning', word: 'Paused', live: false },
  Completed: { tone: 'neutral', word: 'Completed', live: false },
  Cancelled: { tone: 'neutral', word: 'Cancelled', live: false },
  Failed: { tone: 'signal', word: 'Failed', live: false },
}

const PAUSE_LOOKS: Record<PauseReason, JobLook> = {
  'browser-unavailable': { tone: 'warning', word: 'Waiting for browser', live: false },
  review: { tone: 'neutral', word: 'Waiting for your review', live: false },
}

export function jobLook(status: CrawlJobStatus, pauseReason: PauseReason | null = null): JobLook {
  return status === 'Paused' && pauseReason ? PAUSE_LOOKS[pauseReason] : JOB_LOOKS[status]
}

export function percentOf(part: number, max: number | null): number | null {
  return max ? Math.min(100, Math.round((part / max) * 100)) : null
}

export function remainingNote(progress: CrawlProgress): string | null {
  const rate = progress.pagesPerMinute
  if (!rate || progress.status !== 'Running') {
    return null
  }
  const left = progress.maxUrls ? Math.min(progress.queued, progress.maxUrls - progress.crawled) : progress.queued
  return left > 0 ? `About ${formatNumber(Math.ceil(left / rate))} min left at this rate` : null
}
