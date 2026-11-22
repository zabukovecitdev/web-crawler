import type { PathTreeDto, SiteCheckDto, TreeCountsDto, TreeNodeDto } from '@/types/api'

export const browserState = { up: true }

const NONE: TreeCountsDto = { pages: 0, done: 0, failed: 0, skipped: 0, stopped: 0, fetching: 0, queued: 0, oldestModified: null, newestModified: null }

type Spec = { prefix: string; pages: number; from?: string; to?: string; blocked?: boolean; rest?: boolean; children?: Spec[] }

const SITE: Spec[] = [
  { prefix: '/', pages: 1, from: '2026-11-20' },
  {
    prefix: '/guide/',
    pages: 311,
    from: '2026-01-10',
    to: '2026-11-18',
    children: [
      { prefix: '/guide/install', pages: 1, from: '2026-11-18' },
      { prefix: '/guide/quickstart', pages: 1, from: '2026-11-18' },
      { prefix: '/guide/configuration', pages: 1, from: '2026-10-02' },
      { prefix: '/guide/', pages: 308, from: '2026-01-10', to: '2026-11-18', rest: true },
    ],
  },
  {
    prefix: '/reference/',
    pages: 880,
    from: '2026-03-01',
    to: '2026-11-18',
    children: [
      { prefix: '/reference/storage/', pages: 220, from: '2026-09-01', to: '2026-11-18' },
      { prefix: '/reference/search/', pages: 310, from: '2026-06-01', to: '2026-11-18' },
      { prefix: '/reference/crawling/', pages: 350, from: '2026-03-01', to: '2026-11-18' },
    ],
  },
  {
    prefix: '/api/',
    pages: 240,
    from: '2023-05-01',
    to: '2026-11-10',
    children: [
      { prefix: '/api/v2/', pages: 180, from: '2026-11-01', to: '2026-11-10' },
      { prefix: '/api/v1/', pages: 57, from: '2023-05-01', to: '2023-05-30' },
      { prefix: '/api/playground/', pages: 3, from: '2026-11-01', to: '2026-11-10' },
    ],
  },
  {
    prefix: '/blog/',
    pages: 412,
    from: '2016-02-01',
    to: '2026-11-01',
    children: [
      { prefix: '/blog/2026/', pages: 64, from: '2026-01-01', to: '2026-11-01' },
      { prefix: '/blog/2025/', pages: 118, from: '2025-01-01', to: '2025-12-30' },
      { prefix: '/blog/2024/', pages: 130, from: '2024-01-01', to: '2024-12-30' },
      { prefix: '/blog/2016/', pages: 100, from: '2016-02-01', to: '2023-12-30' },
    ],
  },
  { prefix: '/admin/', pages: 50, blocked: true },
]

function countsOf(spec: Spec): TreeCountsDto {
  return { ...NONE, pages: spec.pages, oldestModified: spec.from ? `${spec.from}T00:00:00Z` : null, newestModified: (spec.to ?? spec.from) ? `${spec.to ?? spec.from}T00:00:00Z` : null }
}

function kindOf(spec: Spec): 'Folder' | 'Page' | 'Rest' {
  if (spec.rest) {
    return 'Rest'
  }

  const folder = spec.prefix.endsWith('/') && spec.prefix !== '/'
  const ownPage = spec.pages === 1 && !spec.children
  return folder && !ownPage ? 'Folder' : 'Page'
}

function nodeOf(spec: Spec, withChildren: boolean, counts: (spec: Spec) => TreeCountsDto): TreeNodeDto {
  const kind = kindOf(spec)
  return {
    prefix: spec.prefix,
    kind,
    hasChildren: kind === 'Folder',
    blocked: spec.blocked ?? false,
    counts: counts(spec),
    children: withChildren && spec.children ? spec.children.map((child) => nodeOf(child, false, counts)) : null,
  }
}

function find(prefix: string, specs: Spec[] = SITE): Spec | undefined {
  for (const spec of specs) {
    if (spec.prefix === prefix && spec.children) {
      return spec
    }
    const inside = spec.children ? find(prefix, spec.children) : undefined
    if (inside) {
      return inside
    }
  }
  return undefined
}

function sum(nodes: TreeNodeDto[]): TreeCountsDto {
  return nodes.reduce((total, node) => ({ ...total, pages: total.pages + node.counts.pages }), NONE)
}

export function planTree(host: string, parent: string, counts: (spec: Spec) => TreeCountsDto = countsOf): PathTreeDto {
  const specs = parent === '/' ? SITE : (find(parent)?.children ?? [])
  const nodes = specs.map((spec) => nodeOf(spec, true, counts))
  return { host, parent, total: sum(nodes), nodes }
}

export function jobCounts(spec: Spec): TreeCountsDto {
  const done = spec.blocked ? 0 : Math.floor(spec.pages * 0.6)
  const failed = spec.prefix === '/blog/2026/' ? 2 : 0
  return {
    ...NONE,
    pages: spec.pages,
    done: done - failed,
    failed,
    skipped: spec.blocked ? spec.pages : 0,
    fetching: spec.prefix === '/reference/storage/' ? 1 : 0,
    queued: spec.blocked ? 0 : spec.pages - done,
  }
}

export type MockSite = { sitemap: boolean; needsJs: boolean }

export function siteOf(host: string): MockSite {
  return { sitemap: !/nomap|plain/.test(host), needsJs: /app|spa/.test(host) }
}

export function checkOf(url: string): SiteCheckDto {
  const parsed = new URL(url)
  const site = siteOf(parsed.host)
  const host = parsed.host.startsWith('example.com') ? `docs.${parsed.host}` : parsed.host
  const shell = { textLength: 84, linkCount: 1, emptyAppContainer: true }
  const full = { textLength: 6214, linkCount: 143, emptyAppContainer: false }
  const verdict = site.needsJs ? (browserState.up ? 'Needed' : 'ProbablyNeeded') : 'NotNeeded'
  return {
    requestedUrl: url,
    startUrl: `https://${host}${parsed.pathname}`,
    host,
    redirected: host !== parsed.host,
    startPage: { statusCode: 200, reachable: true, blockedByRobots: false, problem: null },
    robots: { startAllowed: true, crawlDelaySeconds: 1, disallowed: ['/admin/'] },
    sitemap: site.sitemap ? { state: 'Ready', files: 3, urls: 1893, checkedAt: new Date().toISOString() } : { state: 'None', files: 0, urls: 0, checkedAt: new Date().toISOString() },
    browser: { available: browserState.up, reason: browserState.up ? null : 'The browser service did not answer.', checkedAt: new Date().toISOString() },
    javaScript: {
      verdict,
      plain: site.needsJs ? shell : full,
      rendered: verdict === 'Needed' ? { textLength: 5902, linkCount: 131, emptyAppContainer: false } : null,
    },
    checkedAt: new Date().toISOString(),
  }
}
