import { http, HttpResponse } from 'msw'
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

const PRIVATE_HOST = /^(localhost|127\.|10\.|192\.168\.|169\.254\.)/

/** The server's 400 for a bad seed (docs/api-contract.md: http/https only, no loopback or private addresses). */
function seedProblem(seed: string | undefined): string | null {
  try {
    const url = new URL(seed ?? '')
    if (url.protocol !== 'http:' && url.protocol !== 'https:') {
      return 'Seed URLs must use http or https.'
    }
    return PRIVATE_HOST.test(url.hostname) ? 'Seed URLs must not point to a private or loopback address.' : null
  } catch {
    return 'Seed URLs must be absolute http or https addresses.'
  }
}

function badRequest(detail: string): Response {
  return HttpResponse.json({ title: 'Invalid crawl job', detail, status: 400 }, { status: 400 })
}

export const crawlHandlers = [
  http.get('*/api/v1/crawl-jobs', ({ request }) => {
    const limit = Number(new URL(request.url).searchParams.get('limit') ?? 50)
    return json(JOBS.slice(0, limit))
  }),
  http.post('*/api/v1/crawl-jobs', async ({ request }) => {
    const body = (await request.json()) as CreateCrawlJobRequest
    const problem = seedProblem(body.seedUrls?.[0])
    return problem ? badRequest(problem) : json(createJob(body), 201)
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
  http.get('*/api/v1/crawl-jobs/:id/events', ({ params, request }) => {
    const after = Number(new URL(request.url).searchParams.get('after') ?? 0)
    return json(recentEvents(Number(params.id), after))
  }),
]
