import type { CrawlJobStatus, PauseReason, TreeCountsDto } from '@/types/api'
import type { Checked } from './path-rules'

export type StatusTone = 'live' | 'warn' | 'err' | 'ok' | 'info' | 'mute'

export type RowStatus = { text: string; tone: StatusTone; live: boolean }

export type JobContext = { status: CrawlJobStatus; pauseReason: PauseReason | null }

const status = (text: string, tone: StatusTone, live = false): RowStatus => ({ text, tone, live })

export function processedOf(counts: TreeCountsDto): number {
  return counts.done + counts.failed
}

export function remainingOf(counts: TreeCountsDto): number {
  return counts.queued + counts.fetching
}

function pausedStatus(job: JobContext): RowStatus {
  return job.pauseReason === 'browser-unavailable' ? status('Waiting for browser', 'warn') : status('Paused', 'mute')
}

function activeStatus(counts: TreeCountsDto, job: JobContext): RowStatus {
  if (counts.fetching > 0 && job.status === 'Running') {
    return status('Fetching', 'live', true)
  }
  if (job.status === 'Paused') {
    return pausedStatus(job)
  }
  return processedOf(counts) === 0 ? status('Queued', 'mute') : status('In progress', 'mute')
}

export function runStatus(counts: TreeCountsDto, state: Checked, blocked: boolean, job: JobContext): RowStatus {
  if (blocked) {
    return status('Blocked by robots.txt', 'mute')
  }
  if (state === 'false') {
    return processedOf(counts) > 0 ? status('Stopped', 'mute') : status('Not selected', 'mute')
  }
  if (remainingOf(counts) > 0) {
    return activeStatus(counts, job)
  }
  return counts.failed > 0 ? status(`${counts.failed.toLocaleString('en-US')} failed`, 'err') : status('Done', 'ok')
}
