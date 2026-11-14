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
  {
    pageId: pageId('docs.example.com', 4), host: 'docs.example.com', url: 'https://docs.example.com/archive/4',
    title: 'Archive note 4', language: 'en', fetchedAt: ago(4 * HOUR),
    snippet: 'Older note 4 about a <mark>crawl delay</mark> that was changed.',
  },
  {
    pageId: pageId('docs.example.com', 5), host: 'docs.example.com', url: 'https://docs.example.com/archive/5',
    title: 'Archive note 5', language: 'en', fetchedAt: ago(5 * HOUR),
    snippet: 'Older note 5 about a <mark>crawl delay</mark> that was changed.',
  },
  {
    pageId: pageId('docs.example.com', 6), host: 'docs.example.com', url: 'https://docs.example.com/archive/6',
    title: 'Archive note 6', language: 'en', fetchedAt: ago(6 * HOUR),
    snippet: 'Older note 6 about a <mark>crawl delay</mark> that was changed.',
  },
  {
    pageId: pageId('docs.example.com', 7), host: 'docs.example.com', url: 'https://docs.example.com/archive/7',
    title: 'Archive note 7', language: 'en', fetchedAt: ago(7 * HOUR),
    snippet: 'Older note 7 about a <mark>crawl delay</mark> that was changed.',
  },
  {
    pageId: pageId('docs.example.com', 8), host: 'docs.example.com', url: 'https://docs.example.com/archive/8',
    title: 'Archive note 8', language: 'en', fetchedAt: ago(8 * HOUR),
    snippet: 'Older note 8 about a <mark>crawl delay</mark> that was changed.',
  },
  {
    pageId: pageId('docs.example.com', 1), host: 'docs.example.com', url: 'https://docs.example.com/guide/installation',
    title: 'Installation guide', language: 'en', fetchedAt: ago(2 * HOUR),
    snippet: 'Run the crawler &amp; set <mark>crawl delay</mark> in the config file. Never paste &lt;script&gt;alert(1)&lt;/script&gt; into a header value.',
  },
  {
    pageId: pageId('docs.example.com', 2), host: 'docs.example.com', url: 'https://docs.example.com/guide/getting-started',
    title: 'Getting started', language: 'en', fetchedAt: ago(3 * HOUR),
    snippet: 'The first run uses a default <mark>crawl delay</mark> of one second per host. Escaped text such as &amp;lt; stays as written.',
  },
]
function facets(hits: SearchHitDto[]): SearchResponse['hosts'] {
  const counts = new Map<string, number>()
  for (const hit of hits) {
    counts.set(hit.host, (counts.get(hit.host) ?? 0) + 1)
  }
  return [...counts].map(([host, count]) => ({ host, count }))
}

function matches(hit: SearchHitDto, words: string[]): boolean {
  const text = `${hit.title} ${hit.snippet.replace(/<\/?mark>/g, '')}`.toLowerCase()
  return words.some((word) => text.includes(word))
}

function search(url: URL): SearchResponse {
  const host = url.searchParams.get('host')
  const words = (url.searchParams.get('q') ?? '').toLowerCase().split(/\s+/).filter(Boolean)
  const found = words.length > 0 ? HITS.filter((hit) => matches(hit, words)) : []
  const shown = host ? found.filter((hit) => hit.host === host) : found
  return { ...paginate(shown, url, 10), tookMs: 12, hosts: facets(found) }
}

export const searchHandlers = [http.get('*/api/v1/search', ({ request }) => json(search(new URL(request.url))))]
