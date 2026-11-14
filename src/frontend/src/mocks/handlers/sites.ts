import { http } from 'msw'
import type { ExplainDto, PageSummary, RobotsDto, SitemapDto, SitemapUrlDto, SitesResponse, SiteSummary } from '@/types/api'
import { pagesOf } from '../data/pages'
import { findSite, SITES, SITES_OVERVIEW, siteDetail } from '../data/sites'
import { ago, HOUR, json, notFound, paginate } from '../util'

const SORTERS: Record<string, (a: SiteSummary, b: SiteSummary) => number> = {
  host: (a, b) => a.host.localeCompare(b.host),
  pages: (a, b) => b.pages - a.pages,
  failed: (a, b) => b.failed - a.failed,
  lastCrawl: (a, b) => (b.lastCrawlAt ?? '').localeCompare(a.lastCrawlAt ?? ''),
}

function listSites(url: URL): SitesResponse {
  const query = (url.searchParams.get('query') ?? '').toLowerCase()
  const sort = SORTERS[url.searchParams.get('sort') ?? 'lastCrawl'] ?? SORTERS.lastCrawl
  const matching = SITES.filter((s) => s.host.includes(query)).sort(sort)
  return { ...paginate(matching, url, 50), overview: SITES_OVERVIEW }
}

function stateOf(page: PageSummary): string {
  const code = page.statusCode ?? 0
  if (code >= 400 || code === 0) {
    return 'error'
  }
  return code >= 300 ? 'redirect' : 'ok'
}

function listPages(host: string, url: URL) {
  const state = url.searchParams.get('state') ?? 'all'
  const query = (url.searchParams.get('query') ?? '').toLowerCase()
  const items = pagesOf(host)
    .filter((p) => state === 'all' || stateOf(p) === state)
    .filter((p) => `${p.title} ${p.path}`.toLowerCase().includes(query))
  return paginate(items, url)
}

const ROBOTS = `User-agent: *
Disallow: /internal/
Disallow: /api/playground
Crawl-delay: 1

Sitemap: https://docs.example.com/sitemap_index.xml
`

const SITEMAP_URLS: SitemapUrlDto[] = [
  { loc: '/guide/installation', lastModified: '2026-11-11', crawlState: 'Crawled', statusCode: 200, pageId: 1 },
  { loc: '/guide/getting-started', lastModified: '2026-11-09', crawlState: 'Crawled', statusCode: 200, pageId: 2 },
  { loc: '/reference/configuration', lastModified: '2026-11-04', crawlState: 'Crawled', statusCode: 200, pageId: 3 },
  { loc: '/guide/legacy-setup', lastModified: '2025-03-02', crawlState: 'Crawled', statusCode: 404, pageId: 6 },
  { loc: '/internal/draft-roadmap', lastModified: '2026-10-30', crawlState: 'Blocked', statusCode: null, pageId: null },
  { loc: '/reference/limits', lastModified: '2026-11-01', crawlState: 'NotCrawled', statusCode: null, pageId: null },
]

function sitemapOf(host: string, url: URL): SitemapDto {
  const fetchedAt = ago(3 * HOUR)
  return {
    host,
    files: [
      { url: '/sitemap_index.xml', parentUrl: null, kind: 'Index', urlCount: 3, fetchedAt },
      { url: '/sitemap-docs.xml', parentUrl: '/sitemap_index.xml', kind: 'UrlSet', urlCount: 3911, fetchedAt },
      { url: '/sitemap-blog.xml', parentUrl: '/sitemap_index.xml', kind: 'UrlSet', urlCount: 884, fetchedAt },
      { url: '/sitemap-pages.xml', parentUrl: '/sitemap_index.xml', kind: 'UrlSet', urlCount: 107, fetchedAt },
    ],
    urlCount: 4902,
    crawledCount: 4775,
    notInSitemapCount: 37,
    urls: { ...paginate(SITEMAP_URLS, url, 50), total: 4902 },
  }
}

function robotsOf(host: string): RobotsDto {
  return { host, content: ROBOTS, crawlDelayMs: 1000, fetchedAt: ago(3 * HOUR), expiresAt: ago(-21 * HOUR) }
}

export const sitesHandlers = [
  http.get('*/api/v1/hosts', ({ request }) => json(listSites(new URL(request.url)))),
  http.post('*/api/v1/hosts/explain', async ({ request }) => {
    const { url } = (await request.json()) as { url: string }
    const blocked = url.includes('/internal/')
    const body: ExplainDto = {
      url,
      allowedByRobots: !blocked,
      matchedRule: blocked ? 'Disallow: /internal/' : null,
      crawlDelayMs: 1000,
    }
    return json(body)
  }),
  http.get('*/api/v1/hosts/:host', ({ params }) => {
    const site = findSite(String(params.host))
    return site ? json(siteDetail(site)) : notFound()
  }),
  http.get('*/api/v1/hosts/:host/pages', ({ params, request }) =>
    findSite(String(params.host)) ? json(listPages(String(params.host), new URL(request.url))) : notFound(),
  ),
  http.get('*/api/v1/hosts/:host/robots', ({ params }) =>
    findSite(String(params.host)) ? json(robotsOf(String(params.host))) : notFound(),
  ),
  http.get('*/api/v1/hosts/:host/sitemaps', ({ params, request }) =>
    findSite(String(params.host)) ? json(sitemapOf(String(params.host), new URL(request.url))) : notFound(),
  ),
]
