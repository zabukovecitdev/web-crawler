import { apiFetch, queryString } from '@/lib/api'
import type { BrowserStatusDto, PathRule, PathTreeDto, SiteCheckDto, SitemapPlanDto } from '@/types/api'

export { errorCode, errorMessage } from '@/lib/api'

export const BROWSER_UNAVAILABLE = 'browser-unavailable'

export const planKeys = {
  check: (url: string) => ['plan', 'check', url] as const,
  sitemap: (host: string) => ['plan', 'sitemap', host] as const,
  browser: ['plan', 'browser'] as const,
  tree: (host: string, parent: string) => ['plan', 'tree', host, parent] as const,
  jobTree: (jobId: number, parent: string) => ['crawl', 'job', jobId, 'tree', parent] as const,
}

export function checkSite(url: string): Promise<SiteCheckDto> {
  return apiFetch<SiteCheckDto>('/api/v1/site-checks', { method: 'POST', body: JSON.stringify({ url }) })
}

export function fetchSitemapPlan(host: string): Promise<SitemapPlanDto> {
  return apiFetch<SitemapPlanDto>(`/api/v1/hosts/${encodeURIComponent(host)}/plan/sitemap`)
}

export function checkBrowserAgain(): Promise<BrowserStatusDto> {
  return apiFetch<BrowserStatusDto>('/api/v1/browser/check', { method: 'POST' })
}

export function fetchPlanTree(host: string, parent: string): Promise<PathTreeDto> {
  return apiFetch<PathTreeDto>(`/api/v1/hosts/${encodeURIComponent(host)}/plan/tree${queryString({ parent, depth: 2 })}`)
}

export function fetchJobTree(jobId: number, parent: string): Promise<PathTreeDto> {
  return apiFetch<PathTreeDto>(`/api/v1/crawl-jobs/${jobId}/tree${queryString({ parent, depth: 2 })}`)
}

export function putPathRules(jobId: number, rules: PathRule[]): Promise<void> {
  return apiFetch<void>(`/api/v1/crawl-jobs/${jobId}/path-rules`, { method: 'PUT', body: JSON.stringify({ rules }) })
}
