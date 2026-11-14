import { apiFetch, ApiError, queryString } from '@/lib/api'
import type { CrawlEvent, CrawlJobAction, CrawlJobDto, CrawlProgress, CreateCrawlJobRequest } from '@/types/api'

export type StoredEvent = CrawlEvent & { id: number }

const WRITE_HEADERS = { 'X-Requested-With': 'samobot' }
const base = (jobId: number) => `/api/v1/crawl-jobs/${jobId}`

export const crawlKeys = {
  jobs: ['crawl', 'jobs'] as const,
  job: (jobId: number) => ['crawl', 'job', jobId] as const,
  progress: (jobId: number) => ['crawl', 'job', jobId, 'progress'] as const,
  events: (jobId: number) => ['crawl', 'job', jobId, 'events'] as const,
}

export function fetchJobs(): Promise<CrawlJobDto[]> {
  return apiFetch<CrawlJobDto[]>('/api/v1/crawl-jobs?limit=50')
}

export function fetchJob(jobId: number): Promise<CrawlJobDto> {
  return apiFetch<CrawlJobDto>(base(jobId))
}

export function fetchProgress(jobId: number): Promise<CrawlProgress> {
  return apiFetch<CrawlProgress>(`${base(jobId)}/progress`)
}

export function fetchEvents(jobId: number, after?: number): Promise<StoredEvent[]> {
  return apiFetch<StoredEvent[]>(`${base(jobId)}/events${queryString({ after, limit: 200 })}`)
}

export function createJob(request: CreateCrawlJobRequest): Promise<CrawlJobDto> {
  return apiFetch<CrawlJobDto>('/api/v1/crawl-jobs', {
    method: 'POST',
    headers: WRITE_HEADERS,
    body: JSON.stringify(request),
  })
}

export function sendAction(jobId: number, action: CrawlJobAction): Promise<void> {
  return apiFetch<void>(`${base(jobId)}/actions`, {
    method: 'POST',
    headers: WRITE_HEADERS,
    body: JSON.stringify({ action }),
  })
}

type Problem = { title?: string; detail?: string; errors?: Record<string, string[]> }

function problemText(problem: Problem): string | null {
  const fieldErrors = Object.values(problem.errors ?? {}).flat()
  return fieldErrors.length > 0 ? fieldErrors.join(' ') : (problem.detail ?? problem.title ?? null)
}

/** The server's message for an error: a problem document, a JSON string or plain text. */
export function errorMessage(error: unknown): string {
  if (!(error instanceof ApiError)) {
    return error instanceof Error ? error.message : 'Something went wrong.'
  }
  try {
    const parsed: unknown = JSON.parse(error.message)
    if (typeof parsed === 'string') {
      return parsed
    }
    return problemText(parsed as Problem) ?? error.message
  } catch {
    return error.message
  }
}
