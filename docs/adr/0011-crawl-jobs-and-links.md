# 0011. Crawl jobs, links and recrawls

Status: Accepted

## Decision

**Creating and starting (`CrawlJob.Create`, `CrawlJobService`).**
- Invalid seed URLs are dropped. The hosts of the valid seeds become the allowed hosts.
- The seeds are inserted before the job turns Running. A failed insert leaves a Pending job that can start again.

**Completion (`PostgresCrawlJobRepository.CompleteSql`).** A job is finished when no URL is in flight, no URL waits for its first
fetch or a retry, and no fetched page waits for the parser. The parser can still add links to such a page.

**Recrawl, retry and reprocess (`CrawlJob.Reopen`, `CrawlJob.Reprocess`, `IUrlFrontier.Requeue`, `RequeueForReprocess`).**
- `recrawl` fetches every link of the job again. `retry-failed` fetches only the failed links. Recrawling one link makes a
  finished job run again until that link is done.
- A finished job runs again. A running or paused job keeps its status.
- One link of a *cancelled* job cannot be recrawled alone, because running the job again would also fetch everything that was
  still queued. The dashboard offers a new crawl of exactly that link instead: depth 0 and a limit of one page.
- A requeued link is due now, has a clean failure count and no last fetch, so the queue counters, the completion check and the
  dashboard all see it as waiting. A link in flight is left alone. Its fetch history stays in `UrlFetches`.
- `reprocess` processes a completed job again with the current code. A URL whose last fetch was a success of a type that is
  stored now, but was not stored then, is fetched again. `LastCrawlAt` goes back to null, so the job cannot complete before
  that fetch. The last stored fetch of every other URL is parsed again, and that clears its index mark. Nothing stored is
  fetched again.
- The work is given back before the job turns Running, so the completion check never sees a running job without work.

**Link state (`PostgresJobLinkReader.StateExpression`, `LinkStatusDescriber`).**
- The state is one SQL expression, used by the list, its filters and the counts, so they cannot disagree.
- The order of the checks matters: the frontier state first (in flight, waiting for a retry, queued, stopped for good), then the
  last fetch, then the parsed document and its policy.
- A URL that robots.txt or the address policy stopped was never requested, so its fetch step is "skipped", not "done".

**Progress counters (`PostgresJobProgressReader`, `CrawlProgress.From`).**
- A URL counts by its last fetch. Crawled: 2xx. Failed: disabled for a reason other than redirect, robots or blocked address
  (a blocked address counts as skipped). Queued: idle and not yet fetched, or waiting for a retry. In flight: leased.
- While a URL is in flight, `NextAllowedAt` of its host is the in-flight hold, not a next request time, so it is reported as null.
- The rate counts fetches since the start, with a floor of one minute so the first seconds give no spike.

**Content state (`ContentStates`, `ContentStatus`).**
- A URL without a fetch is Failed when it has a fail reason, else NotStored.
- A status below 200 or from 400 up is Failed. 300 to 399 is Redirect.
- A success of a stored type with no stored object is NotStored. A success of any other type is Skipped. A blocked address is
  also Skipped, because the fetch never ran.
- A stored page waits for the parser until it has a parsed document, then for the index until the document is indexed.

**URLs (`UrlNormalizer`, `LinkResolver`, `PageLinkPreparer`).**
- `UrlNormalizer` is the one normalization for every stored URL key (frontier, links, sitemaps, seeds, redirects). It is composed
  of small rules: `IUrlRule` (lower-case scheme and host, drop the fragment, drop the default port) and `IQueryRule` (remove
  tracking parameters, sort the parameters case-insensitively). Add a rule there to change normalization.
- The tracking parameters (`TrackingParameters`) are code, not configuration. `NormalizedUrl` is a stored dedup key, so changing
  the rules changes the key of URLs already stored. A change to the rules needs a data migration that re-normalizes the stored
  keys. `UrlNormalizationCharacterizationTests` pins the output, including known quirks: a repeated parameter is joined
  (`x=1&x=2` gives `x=1%2c2`), a parameter without `=` is dropped, and `?=v` gives `?v`.
- Trailing slashes are kept, because some sites treat `/page` and `/page/` as different resources.
- Links resolve with the `Uri(baseUri, href)` overload. `Uri.TryCreate(href, UriKind.Absolute)` would turn `/path` into a
  `file://` URI on Linux.
- The links table stores every http and https link, internal or external, followed or not. Targets use the same normalization
  as the frontier.
