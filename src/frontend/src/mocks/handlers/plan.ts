import { http, HttpResponse } from 'msw'
import type { PathRule } from '@/types/api'
import { browserState, checkOf, jobCounts, planTree, siteOf } from '../data/plan'
import { json, noContent } from '../util'

let slowPolls = 0

function browserStatus() {
  return {
    available: browserState.up,
    reason: browserState.up ? null : 'The browser service did not answer.',
    checkedAt: new Date().toISOString(),
  }
}

function parentOf(request: Request): string {
  return new URL(request.url).searchParams.get('parent') ?? '/'
}

export const planHandlers = [
  http.post('*/api/v1/site-checks', async ({ request }) => {
    const { url } = (await request.json()) as { url: string }
    return json(checkOf(url))
  }),
  http.get('*/api/v1/browser', () => json(browserStatus())),
  http.post('*/api/v1/browser/check', () => json(browserStatus())),
  http.get('*/api/v1/hosts/:host/plan/sitemap', ({ params }) => {
    const host = decodeURIComponent(String(params.host))
    slowPolls += 1
    if (/slow/.test(host) && slowPolls % 4 !== 0) {
      return json({ state: 'Pending', files: 0, urls: 0, checkedAt: null })
    }
    const ready = siteOf(host).sitemap
    return json({ state: ready ? 'Ready' : 'None', files: ready ? 3 : 0, urls: ready ? 1893 : 0, checkedAt: new Date().toISOString() })
  }),
  http.get('*/api/v1/hosts/:host/plan/tree', ({ params, request }) => json(planTree(decodeURIComponent(String(params.host)), parentOf(request)))),
  http.get('*/api/v1/crawl-jobs/:id/tree', ({ request }) => json(planTree('docs.example.com', parentOf(request), jobCounts))),
  http.put('*/api/v1/crawl-jobs/:id/path-rules', async ({ request }) => {
    const body = (await request.json()) as { rules: PathRule[] }
    return body.rules.some((rule) => !rule.prefix.startsWith('/'))
      ? HttpResponse.json({ title: 'Invalid path rules', status: 400 }, { status: 400 })
      : noContent()
  }),
]

export function setBrowserUp(up: boolean): void {
  browserState.up = up
}
