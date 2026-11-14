import type { CrawlJobDto } from '@/types/api'
import { apiFetch } from './api'

/** The crawl jobs list query key, shared by the crawl screens and the sidebar so they use one cache entry. */
export const crawlJobsKey = ['crawl', 'jobs'] as const

export function fetchCrawlJobs(): Promise<CrawlJobDto[]> {
  return apiFetch<CrawlJobDto[]>('/api/v1/crawl-jobs?limit=50')
}
