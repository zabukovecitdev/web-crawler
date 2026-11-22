import { http, HttpResponse } from 'msw'
import type { CrawlJobAction, CrawlJobDto, CreateCrawlJobRequest } from '@/types/api'
import { JOBS, NO_LINKS, progressOf, recentEvents } from '../data/crawl'
import { browserState } from '../data/plan'
import { ago, json, noContent, notFound } from '../util'

const STATUS_AFTER: Record<CrawlJobAction, CrawlJobDto['status']> = {
  start: 'Running',
  pause: 'Paused',
  cancel: 'Cancelled',
  recrawl: 'Running',
  'retry-failed': 'Running',
  reprocess: 'Running',
}

function findJob(id: unknown): CrawlJobDto | undefined {
  return JOBS.find((job) => job.id === Number(id))
}

function planFields(request: CreateCrawlJobRequest): Pick<CrawlJobDto, 'renderMode' | 'jsNeeded' | 'pathRules' | 'seedFromSitemap' | 'followLinkedPages' | 'reviewAfterPages'> {
  return {
    renderMode: request.renderMode ?? 'Auto',
    jsNeeded: request.jsNeeded ?? false,
    pathRules: request.pathRules ?? [],
    seedFromSitemap: request.seedFromSitemap ?? false,
    followLinkedPages: request.followLinkedPages ?? true,
    reviewAfterPages: request.reviewAfterPages ?? null,
  }
}

function createJob(request: CreateCrawlJobRequest): CrawlJobDto {
  const [seed] = request.seedUrls
  const job: CrawlJobDto = {
    id: Math.max(...JOBS.map((j) => j.id)) + 1,
    status: request.startImmediately ? 'Running' : 'Draft',
    host: new URL(seed).host,
    seedUrls: request.seedUrls,
    maxDepth: request.maxDepth ?? null,
    maxUrls: request.maxUrls ?? null,
    useJsRendering: request.renderMode === 'Always',
    ...planFields(request),
    pauseReason: null,
    pauseDetail: null,
    createdAt: ago(0),
    startedAt: request.startImmediately ? ago(0) : null,
    completedAt: null,
    links: NO_LINKS,
  }
  JOBS.unshift(job)
  return job
}

const PRIVATE_HOST = /^(localhost|127\.|10\.|192\.168\.|169\.254\.)/

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

function needsBrowser(request: CreateCrawlJobRequest): boolean {
  return request.startImmediately === true && (request.renderMode === 'Always' || (request.renderMode === 'Auto' && request.jsNeeded === true))
}

function browserDown(): Response {
  return HttpResponse.json(
    { title: 'The browser service is not available.', status: 409, detail: 'This site needs JavaScript, but the browser service is not available.', code: 'browser-unavailable' },
    { status: 409 },
  )
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
    if (problem) {
      return badRequest(problem)
    }
    return needsBrowser(body) && !browserState.up ? browserDown() : json(createJob(body), 201)
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
  http.get('*/api/v1/crawl-jobs/:id/links', () => json({ total: 0, items: [] })),
  http.post('*/api/v1/crawl-jobs/:id/links/:linkId/recrawl', () => noContent()),
  http.get('*/api/v1/crawl-jobs/:id/progress', ({ params }) => {
    const job = findJob(params.id)
    return job ? json(progressOf(job)) : notFound()
  }),
  http.get('*/api/v1/crawl-jobs/:id/events', ({ params, request }) => {
    const query = new URL(request.url).searchParams
    const after = query.get('after')
    const limit = Math.min(Math.max(Number(query.get('limit') ?? 100), 1), 500)
    return json(recentEvents(Number(params.id), after === null ? undefined : Math.max(Number(after), 0), limit))
  }),
]
