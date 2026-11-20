export type Page<T> = { total: number; items: T[] }

export type Me = { userId: string; name: string; email: string | null; pictureUrl: string | null }
export type GoogleSignInRequest = { credential: string }

export type SiteStatus = 'Crawling' | 'Crawled' | 'Paused' | 'Never'
export type SiteSummary = {
  host: string
  status: SiteStatus
  pages: number
  failed: number
  avgResponseMs: number | null
  crawlDelayMs: number
  lastCrawlAt: string | null
  activeJobId: number | null
}
export type SitesOverview = {
  sites: number
  pages: number
  failed: number
  crawling: number
  medianResponseMs: number | null
  p95ResponseMs: number | null
}
export type SitesResponse = Page<SiteSummary> & { overview: SitesOverview }
export type StatusCount = { statusCode: number; count: number }
export type SiteDetail = SiteSummary & {
  firstCrawlAt: string | null
  statusCodes: StatusCount[]
  contentTypes: { contentType: string; count: number }[]
}

export type ContentState = 'ParsedIndexed' | 'ParsedWaitingForIndex' | 'WaitingForParse' | 'Redirect' | 'Skipped' | 'NotStored' | 'Failed'
export type PageSummary = {
  id: number
  url: string
  path: string
  title: string | null
  statusCode: number | null
  contentType: string | null
  contentLength: number | null
  responseTimeMs: number | null
  depth: number
  lastCrawlAt: string | null
  contentState: ContentState
}
export type FetchRecordDto = {
  fetchedAt: string
  statusCode: number
  contentType: string | null
  contentLength: number | null
  responseTimeMs: number | null
  finalUrl: string | null
}
export type PageDetail = PageSummary & {
  host: string
  crawlJobId: number | null
  nextCrawlAt: string | null
  failReason: string | null
  usedJsRendering: boolean
  bodyTruncated: boolean
  fetches: FetchRecordDto[]
  parsed: ParsedPageDto | null
  foundOn: { pageId: number; url: string } | null
}
export type PageLinkDirection = 'out' | 'in'
export type PageLinkDto = {
  url: string
  text: string | null
  internal: boolean
  noFollow: boolean
  pageId: number | null
  statusCode: number | null
}
export type ParsedPageDto = {
  title: string
  description: string
  language: string
  canonical: string
  robotsDirectives: string
  bodyText: string
  wordCount: number
  headings: { level: number; text: string }[]
  linkCount: number
  internalLinkCount: number
  imageCount: number
  jsonLdBlocks: number
  openGraph: Record<string, string>
}

export type RobotsDto = { host: string; content: string; crawlDelayMs: number | null; fetchedAt: string; expiresAt: string }
export type SitemapFileDto = { url: string; parentUrl: string | null; kind: 'Index' | 'UrlSet'; urlCount: number; fetchedAt: string }
export type SitemapUrlDto = { loc: string; lastModified: string | null; crawlState: 'Crawled' | 'NotCrawled' | 'Blocked'; statusCode: number | null; pageId: number | null }
export type SitemapDto = {
  host: string
  files: SitemapFileDto[]
  urlCount: number
  crawledCount: number
  notInSitemapCount: number
  urls: Page<SitemapUrlDto>
}
export type ExplainDto = { url: string; allowedByRobots: boolean; matchedRule: string | null; crawlDelayMs: number | null }

export type SearchHitDto = {
  pageId: number
  host: string
  url: string
  title: string
  snippet: string
  language: string
  fetchedAt: string | null
}
export type SearchResponse = Page<SearchHitDto> & { tookMs: number; hosts: { host: string; count: number }[] }

export type CrawlJobStatus = 'Draft' | 'Pending' | 'Running' | 'Paused' | 'Completed' | 'Cancelled' | 'Failed'
export type CrawlJobDto = {
  id: number
  status: CrawlJobStatus
  host: string
  seedUrls: string[]
  maxDepth: number | null
  maxUrls: number | null
  useJsRendering: boolean
  createdAt: string
  startedAt: string | null
  completedAt: string | null
  links: JobLinkSummary
}
export type CreateCrawlJobRequest = {
  seedUrls: string[]
  maxDepth?: number
  maxUrls?: number
  useJsRendering?: boolean
  startImmediately?: boolean
}
export type CrawlJobAction = 'start' | 'pause' | 'cancel' | 'recrawl' | 'retry-failed' | 'reprocess'

export type LinkState = 'Queued' | 'Fetching' | 'Retrying' | 'Parsing' | 'Indexing' | 'Indexed' | 'NotIndexed' | 'Skipped' | 'Failed'
export type LinkStepStatus = 'Pending' | 'Waiting' | 'Active' | 'Done' | 'Skipped' | 'Failed'
export type LinkSteps = { fetch: LinkStepStatus; store: LinkStepStatus; parse: LinkStepStatus; index: LinkStepStatus }
export type JobLinkDto = {
  id: number
  url: string
  host: string
  depth: number
  title: string | null
  state: LinkState
  reason: string
  steps: LinkSteps
  statusCode: number | null
  failCount: number
  lastCrawlAt: string | null
  nextCrawlAt: string | null
  canRecrawl: boolean
}
export type JobLinkFilter = 'all' | 'problems' | 'notIndexed' | 'pending' | 'indexed'
export type LinkProblem = { reason: string; label: string; count: number }
export type JobLinkSummary = {
  total: number
  queued: number
  fetching: number
  retrying: number
  parsing: number
  indexing: number
  indexed: number
  notIndexed: number
  skipped: number
  failed: number
  problems: LinkProblem[]
}
export type CrawlProgress = {
  jobId: number
  status: CrawlJobStatus
  crawled: number
  queued: number
  inFlight: number
  failed: number
  maxUrls: number | null
  currentUrl: string | null
  currentDepth: number | null
  crawlDelayMs: number
  delaySource: 'default' | 'robots.txt' | 'adaptive'
  nextRequestAt: string | null
  avgResponseMs: number | null
  maxResponseMs: number | null
  pagesPerMinute: number | null
  startedAt: string | null
  statusCounts: { ok: number; redirect: number; clientError: number; serverError: number }
}
export type ContentStatus = 'Stored' | 'Skipped' | 'NotStored' | 'Failed'

export type CrawlEvent =
  | { id: number; crawlJobId: number; eventType: 'JobCreated' | 'JobStarted' | 'JobPaused' | 'JobCancelled' | 'JobCompleted' | 'JobRecrawlRequested'; at: string }
  | { id: number; crawlJobId: number; eventType: 'FetchStarted'; at: string; url: string; depth: number }
  | {
      id: number
      crawlJobId: number
      eventType: 'FetchCompleted'
      at: string
      pageId?: number
      host?: string
      url: string
      depth: number
      statusCode: number
      responseTimeMs: number | null
      contentType: string | null
      contentLength: number | null
      contentStatus: ContentStatus
      delayMs: number
      outcome: string
    }

export type AnalyticsRange = '24h' | '7d' | '30d' | 'all'
export type AnalyticsOverview = {
  hosts: number
  pages: number
  requests: number
  bytes: number
  failedPages: number
  failureRate: number
  p50ResponseMs: number | null
  p95ResponseMs: number | null
  maxResponseMs: number | null
  from: string | null
  to: string | null
}
export type HostAnalytics = {
  host: string
  pages: number
  requests: number
  p50ResponseMs: number | null
  p95ResponseMs: number | null
  maxResponseMs: number | null
  bytes: number
  failureRate: number
  crawlDelayMs: number
}
export type StatusClassCounts = { ok: number; redirect: number; clientError: number; serverError: number }
export type ContentTypeCount = { contentType: string; requests: number; stored: boolean }
export type TimeseriesPoint = { bucket: string; requests: number }
