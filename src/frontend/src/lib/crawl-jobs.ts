import type { CrawlJobDto } from '@/types/api'
import { apiFetch } from './api'

export const crawlJobsKey = ['crawl', 'jobs'] as const

export function fetchCrawlJobs(): Promise<CrawlJobDto[]> {
  return apiFetch<CrawlJobDto[]>('/api/v1/crawl-jobs?limit=50')
}
