import type { SiteDetail, SiteSummary, SitesOverview } from '@/types/api'
import { HOUR, MINUTE, ago } from '../util'

// Sample data from docs/design/Main.dc.html. docs.example.com is crawled live by job #42.
export const LIVE_JOB_ID = 42

export const SITES: SiteSummary[] = [
  { host: 'docs.example.com', status: 'Crawling', pages: 4812, failed: 37, avgResponseMs: 184, crawlDelayMs: 1000, lastCrawlAt: ago(5_000), activeJobId: LIVE_JOB_ID },
  { host: 'example.org', status: 'Crawled', pages: 3204, failed: 12, avgResponseMs: 142, crawlDelayMs: 1000, lastCrawlAt: ago(2 * HOUR), activeJobId: null },
  { host: 'blog.example.net', status: 'Crawled', pages: 2118, failed: 4, avgResponseMs: 231, crawlDelayMs: 2000, lastCrawlAt: ago(30 * HOUR), activeJobId: null },
  { host: 'shop.example.com', status: 'Crawled', pages: 1536, failed: 96, avgResponseMs: 418, crawlDelayMs: 5000, lastCrawlAt: ago(76 * HOUR), activeJobId: null },
  { host: 'status.example.io', status: 'Crawled', pages: 642, failed: 0, avgResponseMs: 96, crawlDelayMs: 1000, lastCrawlAt: ago(100 * HOUR), activeJobId: null },
  { host: 'wiki.example.edu', status: 'Paused', pages: 170, failed: 21, avgResponseMs: 612, crawlDelayMs: 10000, lastCrawlAt: ago(120 * HOUR), activeJobId: null },
]

export const SITES_OVERVIEW: SitesOverview = {
  sites: 6,
  pages: 12482,
  failed: 170,
  crawling: 1,
  medianResponseMs: 184,
  p95ResponseMs: 612,
}

export function siteDetail(site: SiteSummary): SiteDetail {
  return {
    ...site,
    firstCrawlAt: ago(40 * 24 * HOUR + 17 * MINUTE),
    statusCodes: [
      { statusCode: 200, count: Math.round(site.pages * 0.93) },
      { statusCode: 301, count: Math.round(site.pages * 0.05) },
      { statusCode: 404, count: Math.max(site.failed - 3, 0) },
      { statusCode: 503, count: Math.min(site.failed, 3) },
    ],
    contentTypes: [
      { contentType: 'text/html', count: Math.round(site.pages * 0.9) },
      { contentType: 'application/pdf', count: Math.round(site.pages * 0.04) },
      { contentType: 'application/xml', count: Math.round(site.pages * 0.01) },
    ],
  }
}

export function findSite(host: string): SiteSummary | undefined {
  return SITES.find((site) => site.host === host)
}
