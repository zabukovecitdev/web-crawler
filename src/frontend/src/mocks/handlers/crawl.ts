import { http } from 'msw'
import type { CrawlJobAction, CrawlJobDto, CreateCrawlJobRequest } from '@/types/api'
import { JOBS, progressOf, recentEvents } from '../data/crawl'
import { ago, json, noContent, notFound } from '../util'

const STATUS_AFTER: Record<CrawlJobAction, CrawlJobDto['status']> = {
  start: 'Running',
  pause: 'Paused',
  cancel: 'Cancelled',
}

function findJob(id: unknown): CrawlJobDto | undefined {
  return JOBS.find((job) => job.id === Number(id))
}

function createJob(request: CreateCrawlJobRequest): CrawlJobDto {
  const [seed] = request.seedUrls
  const job: CrawlJobDto = {
    id: Math.max(...JOBS.map((j) => j.id)) + 1,
    status: request.startImmediately ? 'Running' : 'Draft',
    host: new URL(seed).host,
    seedUrls: request.seedUrls,
    maxDepth: request.maxDepth ?? 5,
    maxUrls: request.maxUrls ?? 5000,
    useJsRendering: request.useJsRendering ?? false,
    respectRobots: request.respectRobots ?? true,
    createdAt: ago(0),
    startedAt: request.startImmediately ? ago(0) : null,
    completedAt: null,
  }
  JOBS.unshift(job)
  return job
}

export const crawlHandlers = [
  http.get('*/api/v1/crawl-jobs', ({ request }) => {
    const limit = Number(new URL(request.url).searchParams.get('limit') ?? 50)
    return json(JOBS.slice(0, limit))
  }),
  http.post('*/api/v1/crawl-jobs', async ({ request }) => {
    return json(createJob((await request.json()) as CreateCrawlJobRequest), 201)
  }),
  http.get('*/api/v1/crawl-jobs/:id', ({ params }) => {
    const job = findJob(params.id)
    return job ? json(job) : notFound()
  }),
  http.post('*/api/v1/crawl-jobs/:id/actions', async ({ params, request }) => {
    const job = findJob(params.id)
    if (!job) {
      return notFound()
    }
    const { action } = (await request.json()) as { action: CrawlJobAction }
    job.status = STATUS_AFTER[action]
    return noContent()
  }),
  http.get('*/api/v1/crawl-jobs/:id/progress', ({ params }) => {
    const job = findJob(params.id)
    return job ? json(progressOf(job)) : notFound()
  }),
  http.get('*/api/v1/crawl-jobs/:id/events', ({ params }) => json(recentEvents(Number(params.id)))),
]
