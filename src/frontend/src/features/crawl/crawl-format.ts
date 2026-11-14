import type { ContentStatus, CrawlJobStatus, CrawlProgress } from '@/types/api'
import { formatNumber } from '@/lib/format'
import type { Tone } from '@/components/shared/status'

const DASH = '—'

/** 400 -> "0.4 s". */
export function formatSeconds(ms: number | null): string {
  return ms === null ? DASH : `${(Math.max(0, ms) / 1000).toFixed(1)} s`
}

/** 1452000 -> "24 min 12 s". */
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

export function delaySourceWord(source: CrawlProgress['delaySource']): string {
  return source === 'robots.txt' ? 'robots.txt Crawl-delay' : 'Default delay'
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

export function jobLook(status: CrawlJobStatus): JobLook {
  return JOB_LOOKS[status]
}

export function percentOf(part: number, max: number | null): number | null {
  return max ? Math.min(100, Math.round((part / max) * 100)) : null
}

/** "About 59 min left at this rate", or null when the rate is unknown. */
export function remainingNote(progress: CrawlProgress): string | null {
  const rate = progress.pagesPerMinute
  if (!rate || progress.status !== 'Running') {
    return null
  }
  const left = progress.maxUrls ? Math.min(progress.queued, progress.maxUrls - progress.crawled) : progress.queued
  return left > 0 ? `About ${formatNumber(Math.ceil(left / rate))} min left at this rate` : null
}
