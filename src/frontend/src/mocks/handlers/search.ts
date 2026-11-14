import { http } from 'msw'
import type { SearchHitDto, SearchResponse } from '@/types/api'
import { pageId } from '../data/pages'
import { ago, HOUR, json, MINUTE, paginate } from '../util'

// Results from docs/design/Search.dc.html ("crawl delay").
const HITS: SearchHitDto[] = [
  {
    pageId: pageId('docs.example.com', 3), host: 'docs.example.com', url: 'https://docs.example.com/reference/configuration',
    title: 'Configuration reference', language: 'en', fetchedAt: ago(MINUTE),
    snippet: 'Set the <mark>crawl delay</mark> for each host in milliseconds. When robots.txt gives a longer <mark>crawl delay</mark>, the crawler uses the longer value.',
  },
  {
    pageId: pageId('docs.example.com', 9), host: 'docs.example.com', url: 'https://docs.example.com/guide/politeness',
    title: 'How politeness works', language: 'en', fetchedAt: ago(5 * MINUTE),
    snippet: 'The host row holds the time of the next allowed request. The <mark>crawl delay</mark> is added after each response, so one host never gets two requests at the same time.',
  },
  {
    pageId: pageId('blog.example.net', 10), host: 'blog.example.net', url: 'https://blog.example.net/2026/why-your-crawler-was-blocked',
    title: 'Why your crawler was blocked', language: 'en', fetchedAt: ago(30 * HOUR),
    snippet: 'Most sites block a crawler that ignores the Crawl-delay line. A short <mark>delay</mark> between requests is the easiest way to stay welcome.',
  },
  {
    pageId: pageId('wiki.example.edu', 11), host: 'wiki.example.edu', url: 'https://wiki.example.edu/wiki/Robots_exclusion_standard',
    title: 'Robots exclusion standard', language: 'en', fetchedAt: ago(120 * HOUR),
    snippet: 'The Crawl-delay directive is not part of the standard, but many crawlers read it as the number of seconds to wait between requests.',
  },
]

function facets(hits: SearchHitDto[]): SearchResponse['hosts'] {
  const counts = new Map<string, number>()
  for (const hit of hits) {
    counts.set(hit.host, (counts.get(hit.host) ?? 0) + 1)
  }
  return [...counts].map(([host, count]) => ({ host, count }))
}

function search(url: URL): SearchResponse {
  const host = url.searchParams.get('host')
  const q = (url.searchParams.get('q') ?? '').trim()
  const found = q ? HITS : []
  const facet = facets(found)
  const shown = host ? found.filter((hit) => hit.host === host) : found
  return { ...paginate(shown, url, 10), tookMs: 12, hosts: facet }
}

export const searchHandlers = [http.get('*/api/v1/search', ({ request }) => json(search(new URL(request.url))))]
