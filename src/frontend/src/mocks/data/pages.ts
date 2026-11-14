import type { ContentState, PageDetail, PageSummary, ParsedPageDto } from '@/types/api'
import { MINUTE, ago } from '../util'
import { SITES } from './sites'

type Template = {
  n: number
  title: string
  path: string
  code: number
  type: string
  length: number
  ms: number
  depth: number
  state: ContentState
}

// Rows from docs/design/Site.dc.html, plus the pages the search screen links to.
const TEMPLATES: Template[] = [
  { n: 1, title: 'Installation guide', path: '/guide/installation', code: 200, type: 'text/html', length: 48200, ms: 172, depth: 2, state: 'ParsedIndexed' },
  { n: 2, title: 'Getting started', path: '/guide/getting-started', code: 200, type: 'text/html', length: 36900, ms: 158, depth: 1, state: 'ParsedIndexed' },
  { n: 3, title: 'Configuration reference', path: '/reference/configuration', code: 200, type: 'text/html', length: 112400, ms: 264, depth: 2, state: 'ParsedWaitingForIndex' },
  { n: 4, title: 'Moved: upgrade notes', path: '/upgrade', code: 301, type: 'text/html', length: 300, ms: 61, depth: 2, state: 'Redirect' },
  { n: 5, title: 'API: crawl jobs', path: '/api/crawl-jobs', code: 200, type: 'text/html', length: 74000, ms: 201, depth: 3, state: 'ParsedIndexed' },
  { n: 6, title: 'Not found', path: '/guide/legacy-setup', code: 404, type: 'text/html', length: 2100, ms: 88, depth: 4, state: 'NotStored' },
  { n: 7, title: 'User manual (PDF)', path: '/downloads/manual.pdf', code: 200, type: 'application/pdf', length: 2400000, ms: 912, depth: 3, state: 'Skipped' },
  { n: 8, title: 'Server error', path: '/api/playground', code: 503, type: 'text/html', length: 900, ms: 5004, depth: 3, state: 'Failed' },
  { n: 9, title: 'How politeness works', path: '/guide/politeness', code: 200, type: 'text/html', length: 29800, ms: 149, depth: 2, state: 'ParsedIndexed' },
  { n: 10, title: 'Why your crawler was blocked', path: '/2026/why-your-crawler-was-blocked', code: 200, type: 'text/html', length: 41000, ms: 231, depth: 1, state: 'ParsedIndexed' },
  { n: 11, title: 'Robots exclusion standard', path: '/wiki/Robots_exclusion_standard', code: 200, type: 'text/html', length: 88000, ms: 612, depth: 2, state: 'ParsedIndexed' },
]

/** Page id = host index * 100 + template number, so ids are stable across reloads. */
export function pageId(host: string, n: number): number {
  return SITES.findIndex((site) => site.host === host) * 100 + n
}

function summary(host: string, t: Template): PageSummary {
  return {
    id: pageId(host, t.n),
    url: `https://${host}${t.path}`,
    path: t.path,
    title: t.title,
    statusCode: t.code,
    contentType: t.type,
    contentLength: t.length,
    responseTimeMs: t.ms,
    depth: t.depth,
    lastCrawlAt: ago(t.n * MINUTE),
    contentState: t.state,
  }
}

export function pagesOf(host: string): PageSummary[] {
  return TEMPLATES.map((t) => summary(host, t))
}

const PARSED: ParsedPageDto = {
  title: 'Configuration reference',
  description: 'Every setting the crawler reads, with defaults and examples.',
  language: 'en',
  canonical: 'https://docs.example.com/reference/configuration',
  robotsDirectives: 'index, follow',
  bodyText:
    'Set the crawl delay for each host in milliseconds. When robots.txt gives a longer crawl delay, the crawler uses the longer value.',
  wordCount: 2140,
  headings: [
    { level: 1, text: 'Configuration reference' },
    { level: 2, text: 'Crawl delay' },
    { level: 2, text: 'Page limits' },
  ],
  linkCount: 64,
  internalLinkCount: 52,
  imageCount: 3,
  jsonLdBlocks: 1,
  openGraph: { 'og:title': 'Configuration reference', 'og:type': 'article' },
}

export function pageDetail(id: number): PageDetail | null {
  const site = SITES[Math.floor(id / 100)]
  const template = TEMPLATES.find((t) => t.n === id % 100)
  if (!site || !template) {
    return null
  }
  const base = summary(site.host, template)
  const stored = template.state.startsWith('Parsed')
  return {
    ...base,
    host: site.host,
    crawlJobId: site.activeJobId,
    nextCrawlAt: null,
    failReason: template.state === 'Failed' ? 'HTTP 503 after 3 attempts' : null,
    usedJsRendering: false,
    fetches: [
      { fetchedAt: ago(template.n * MINUTE), statusCode: template.code, contentType: template.type, contentLength: template.length, responseTimeMs: template.ms, finalUrl: null },
      { fetchedAt: ago(26 * 60 * MINUTE), statusCode: template.code, contentType: template.type, contentLength: template.length, responseTimeMs: template.ms + 30, finalUrl: null },
    ],
    parsed: stored ? { ...PARSED, title: template.title, canonical: base.url } : null,
  }
}
