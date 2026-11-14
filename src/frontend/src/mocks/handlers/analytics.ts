import { http } from 'msw'
import type { AnalyticsOverview, ContentTypeCount, HostAnalytics, StatusClassCounts, TimeseriesPoint } from '@/types/api'
import { ago, HOUR, json } from '../util'

// Numbers from docs/design/Analytics.dc.html.
const HOSTS: HostAnalytics[] = [
  { host: 'docs.example.com', pages: 4812, requests: 5904, p50ResponseMs: 184, p95ResponseMs: 520, maxResponseMs: 5004, bytes: 711e6, failureRate: 0.008, crawlDelayMs: 1000 },
  { host: 'example.org', pages: 3204, requests: 3790, p50ResponseMs: 142, p95ResponseMs: 388, maxResponseMs: 1940, bytes: 436e6, failureRate: 0.004, crawlDelayMs: 1000 },
  { host: 'blog.example.net', pages: 2118, requests: 2390, p50ResponseMs: 231, p95ResponseMs: 640, maxResponseMs: 3104, bytes: 288e6, failureRate: 0.002, crawlDelayMs: 2000 },
  { host: 'shop.example.com', pages: 1536, requests: 1920, p50ResponseMs: 418, p95ResponseMs: 1310, maxResponseMs: 6212, bytes: 402e6, failureRate: 0.063, crawlDelayMs: 5000 },
  { host: 'status.example.io', pages: 642, requests: 688, p50ResponseMs: 96, p95ResponseMs: 210, maxResponseMs: 402, bytes: 38e6, failureRate: 0, crawlDelayMs: 1000 },
  { host: 'wiki.example.edu', pages: 170, requests: 214, p50ResponseMs: 612, p95ResponseMs: 2400, maxResponseMs: 9870, bytes: 31e6, failureRate: 0.124, crawlDelayMs: 10000 },
]

const OVERVIEW: AnalyticsOverview = {
  hosts: 6,
  pages: 12482,
  requests: 14906,
  bytes: 1906e6,
  failedPages: 170,
  failureRate: 0.0114,
  p50ResponseMs: 184,
  p95ResponseMs: 612,
  maxResponseMs: 9870,
  from: ago(24 * HOUR),
  to: ago(0),
}

const STATUS: StatusClassCounts = { ok: 13890, redirect: 840, clientError: 148, serverError: 28 }

const CONTENT_TYPES: ContentTypeCount[] = [
  { contentType: 'text/html', requests: 13120, stored: true },
  { contentType: 'application/pdf', requests: 880, stored: false },
  { contentType: 'application/xml', requests: 410, stored: false },
  { contentType: 'image/png', requests: 310, stored: false },
  { contentType: 'application/json', requests: 186, stored: false },
]

const PER_HOUR = [0, 0, 0, 0, 0, 0, 12, 40, 36, 20, 0, 0, 180, 420, 96, 0, 0, 640, 1210, 1480, 1390, 1820, 2460, 3180]

const DAY = 24 * HOUR

/** Hour buckets for 24h, otherwise day buckets; scaled down for a single host. */
function timeseries(url: URL): TimeseriesPoint[] {
  const bucket = url.searchParams.get('bucket')
  const share = hostShare(url.searchParams.get('host'))
  if (bucket !== 'day') {
    return PER_HOUR.map((n, i) => ({ bucket: ago((PER_HOUR.length - 1 - i) * HOUR), requests: Math.round(n * share) }))
  }
  const days = { '7d': 7, '30d': 30 }[url.searchParams.get('range') ?? ''] ?? 45
  return Array.from({ length: days }, (_, i) => ({
    bucket: ago((days - 1 - i) * DAY),
    requests: Math.round((300 + ((i * 7919) % 900) + (i === days - 1 ? 3180 : 0)) * share),
  }))
}

function hostShare(host: string | null): number {
  const row = HOSTS.find((h) => h.host === host)
  return row ? row.requests / OVERVIEW.requests : 1
}

function scaled<T extends Record<string, number>>(counts: T, share: number): T {
  return Object.fromEntries(Object.entries(counts).map(([k, v]) => [k, Math.round(v * share)])) as T
}

const hostParam = (req: Request) => hostShare(new URL(req.url).searchParams.get('host'))

export const analyticsHandlers = [
  http.get('*/api/v1/analytics/overview', () => json(OVERVIEW)),
  http.get('*/api/v1/analytics/hosts', () => json(HOSTS)),
  http.get('*/api/v1/analytics/status-codes', ({ request }) => json(scaled(STATUS, hostParam(request)))),
  http.get('*/api/v1/analytics/content-types', ({ request }) =>
    json(CONTENT_TYPES.map((c) => ({ ...c, requests: Math.round(c.requests * hostParam(request)) }))),
  ),
  http.get('*/api/v1/analytics/timeseries', ({ request }) => json(timeseries(new URL(request.url)))),
]
