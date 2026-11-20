# 0008. Sitemaps

Status: Accepted

## Decision

**Queue (`PostgresSitemapStore`, `SitemapBatchHandler`, `SitemapEnqueueSchedule`).**
- One row in `SitemapRequests` per host is the queue. A claim sets a lease with `FOR UPDATE SKIP LOCKED`, like the other work
  queues. A refresh replaces the files of a host in one transaction.
- The loop runs every second but queues finished hosts at most once per `SitemapEnqueueSchedule.Interval`. It queues the hosts
  of completed jobs.
- A host that failed keeps its queue row, so another run picks it up when the lease ends: the lease is the backoff. After
  `MaxAttempts` claims without a finished walk, the host waits for the next weekly refresh.
- A refresh requested while a walk runs stays queued (`CompletedAt` NULL), so the request is not lost.
- `SitemapClaim.LeaseUntil` is the ownership token. `Complete` writes only while the queue row still holds exactly that value,
  and returns false otherwise.
- **The lease is 2 hours.** A walk fetches at most 50 files. Each fetch waits at most 60 s for the host delay
  (`SitemapLimits.MaxFetchWait`) and then downloads under `Crawler:SitemapTimeoutSeconds` (30 s by default, 60 s when the
  lease was chosen). That is at most 50 x (60 s + 60 s) = 100 minutes, plus the robots.txt fetch. Two hours leave room, so a
  running walk keeps its lease.
- **Fetch slots.** `ReserveFetchSlot` is one statement. The row lock of the UPDATE serializes callers, and RETURNING sees the new
  value, so the booked slot is the new `NextAllowedAt` minus the delay. A missing host row returns no row, which means no wait.

**Walk (`SitemapDiscovery`, `SitemapRefresher`).**
- The sitemap URLs come from the `Sitemap:` lines of robots.txt, else `/sitemap.xml`.
- The walk takes the seeds first, then the children of the indexes: at most 2 levels and 50 downloads. Failed or unparsable
  children count against the limit.
- Each fetch waits for the host delay and respects robots.txt.
- A walk that found a file, or found only 404 and 410 answers, replaces the stored data. `Gone` means that the walk downloaded
  at least one URL and every download answered 404 or 410. A walk with no files and only transient failures keeps the old data.

**Download and parse (`HttpSitemapSource`, `SitemapParser`, `SitemapLimits`).**
- The sitemap client follows no redirects and does not decompress. The parser sees the raw bytes and caps the decompressed size
  itself. A body over 10 MB, a network error and a timeout give status 0. `ISitemapSource` never throws for HTTP or network errors.
- A gzip body is streamed into the XML reader, capped at 50 MB. A larger stream (a gzip bomb) gives null.
- DTDs are prohibited and no resolver is set, so the parser is safe against XXE. Only http and https URLs of the same host stay. A
  file keeps at most 50,000 entries.

**Overview (`PostgresSitemapReader`, `SitemapOverviewService`).** A sitemap URL counts as crawled when a page of the host with
the same normalized URL has a fetch. When several jobs hold that page, the newest crawl wins. A URL that is not crawled shows as
Blocked when robots.txt disallows it for our user agent. `SitemapEntry.NormalizedLoc` is the key to the crawled pages.
