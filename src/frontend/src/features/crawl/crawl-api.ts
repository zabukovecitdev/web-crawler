import { apiFetch, queryString } from '@/lib/api'
import { crawlJobsKey, fetchCrawlJobs } from '@/lib/crawl-jobs'
import type { CrawlEvent, CrawlJobAction, CrawlJobDto, CrawlProgress, CreateCrawlJobRequest } from '@/types/api'

export { errorMessage } from '@/lib/api'

export type StoredEvent = CrawlEvent & { id: number }

/** A hub event: the server also sends the stored id (types/api.ts does not list it). The mock stream has none. */
export type LiveEvent = CrawlEvent & { id?: number }

const PAGE_SIZE = 200
const MAX_PAGES = 5
const base = (jobId: number) => `/api/v1/crawl-jobs/${jobId}`

export const crawlKeys = {
  jobs: crawlJobsKey,
  job: (jobId: number) => ['crawl', 'job', jobId] as const,
  progress: (jobId: number) => ['crawl', 'job', jobId, 'progress'] as const,
  events: (jobId: number) => ['crawl', 'job', jobId, 'events'] as const,
}

export const fetchJobs = fetchCrawlJobs

export function fetchJob(jobId: number): Promise<CrawlJobDto> {
  return apiFetch<CrawlJobDto>(base(jobId))
}

export function fetchProgress(jobId: number): Promise<CrawlProgress> {
  return apiFetch<CrawlProgress>(`${base(jobId)}/progress`)
}

/** One page of events. Without `after` the server returns the newest page (tail mode). */
export function fetchEvents(jobId: number, after?: number): Promise<StoredEvent[]> {
  return apiFetch<StoredEvent[]>(`${base(jobId)}/events${queryString({ after, limit: PAGE_SIZE })}`)
}

/**
 * Events newer than `cursor`, ascending. Cursor 0 means none seen yet: the newest page (tail mode).
 * Otherwise pages on while a page is full, up to MAX_PAGES.
 */
export async function fetchEventsSince(jobId: number, cursor: number): Promise<StoredEvent[]> {
  if (cursor <= 0) {
    return fetchEvents(jobId)
  }
  const all: StoredEvent[] = []
  let after = cursor
  for (let page = 0; page < MAX_PAGES; page += 1) {
    const events = await fetchEvents(jobId, after)
    all.push(...events)
    if (events.length < PAGE_SIZE) {
      break
    }
    after = events[events.length - 1].id
  }
  return all
}

export function createJob(request: CreateCrawlJobRequest): Promise<CrawlJobDto> {
  return apiFetch<CrawlJobDto>('/api/v1/crawl-jobs', { method: 'POST', body: JSON.stringify(request) })
}

export function sendAction(jobId: number, action: CrawlJobAction): Promise<void> {
  return apiFetch<void>(`${base(jobId)}/actions`, { method: 'POST', body: JSON.stringify({ action }) })
}
