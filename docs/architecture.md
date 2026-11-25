# Architecture

How SamoBot works and which rules the build enforces. How to run and configure it is in [operations.md](operations.md). The
reasons behind each part are in the [ADRs](adr/README.md).

## The claim pattern

Every stage does the same three steps: claim rows with a lease, do the work, record one result.

- **Crawl:** a claim takes at most one due URL for each host and locks the host row. The host row also holds the
  delay until the next request, so politeness needs no extra service.
- **Parse:** a claim leases stored fetches with `FOR UPDATE SKIP LOCKED`. One transaction stores the page, replaces the links of
  the page in `Links` (all of them, for the link lists of the dashboard), adds the in-scope links and marks the fetch parsed.
- **Index:** pages with no `IndexedAt` go to Meilisearch in one batch. Indexing is idempotent.

- **Stored formats:** HTML, Markdown and plain text (`StoredMediaTypes`). Page requests send `Accept` with HTML first,
  then Markdown, then plain text. Markdown and plain text are converted to HTML at parse time. Other types are fetched
  and skipped.
- **Recrawl:** the job action `recrawl` runs a completed job again with the current code. It fetches again the URLs
  whose body was skipped for a type that is stored now, and parses (and so indexes) the stored pages again.

A worker that dies leaves a lease. When the lease ends, the next claim takes the row again.
A job is complete when no URL is in flight, no URL waits for its first fetch or a retry, and no page waits for the parser.

Decisions: [ADR 0004](adr/0004-frontier-pacing-and-politeness.md), [ADR 0010](adr/0010-postgres-schema-and-sql.md),
[ADR 0011](adr/0011-crawl-jobs-and-links.md).

## Network safety (SSRF)

Seed URLs are checked as text (`SeedUrlValidator`). The real defence runs at connect time, because a public name can
resolve to a private address (DNS rebinding) and a redirect can point at one.

- **Connect check:** the clients of the fetcher, of robots.txt and of sitemaps use `SafeConnect`, a `SocketsHttpHandler.ConnectCallback`.
  It resolves all addresses, drops those that `IpAddressPolicy` denies (private, loopback, link-local and metadata,
  CGNAT, multicast, reserved, documentation, and IPv6 forms that embed such an IPv4 address) and connects to the first
  allowed one. No allowed address gives `BlockedAddressException`. Every redirect hop opens its own connection, so every
  hop is checked. A new HTTP client for the open web must be added with `AddSafeHttpClient`.
- **Blocked URLs** end as `Disabled` with the reason `blocked-address`: no retry, no recrawl. The job progress counts them
  as skipped, not failed.
- **Address cache:** a successful lookup stores the allowed addresses in `Hosts.ResolvedAddresses` and `ResolvedAt`
  (`IHostAddressCache`, with an in-memory layer in front). While `ResolvedAt` is younger than `Crawler:DnsCacheMinutes`
  (default 60, 0 turns it off) a connect uses the cached addresses and asks no DNS. Every cached address is checked with
  `IpAddressPolicy` again on every use. When no cached address connects, the host is resolved once more, the cache is
  updated and the connect is retried. When DNS gives only denied addresses, the cache is cleared and the connect is
  blocked. The connection is made by IP, but the request URL is never rewritten, so the Host header and the TLS server
  name stay the host name. The cache key is the DNS name; a name with no `Hosts` row is cached in memory only.
- **Browser rendering:** Chrome connects over CDP and cannot use the callback. Every request of the page (navigation,
  redirects, sub-resources) is routed through Playwright, and its host must resolve only to allowed addresses. Chrome
  resolves the name again itself, so a short rebinding window remains. Run Chrome in a network that cannot reach
  private ranges (see [JavaScript rendering](operations.md#javascript-rendering)).
- **Configuration:** `Crawler:AllowPrivateAddresses` (default false; development and tests only, logs a startup warning),
  `Crawler:UseProxy` (default false; a proxy would hide the target from the connect check),
  `Crawler:DnsCacheMinutes` (default 60).

Decisions: [ADR 0002](adr/0002-network-safety.md).

## Timeouts

`HttpClient.Timeout` covers only the headers when the body is streamed, so the HTTP clients have none (infinite) and every
request runs under a `RequestDeadline`: one total deadline for the whole request, the body read included, and a shorter
first-byte deadline until the response headers arrive. A timeout gives status 0 with the error `timeout` (the robots and
sitemap sources treat it like any network error). A cancellation by the worker's shutdown token is not a timeout and still
propagates. A value of 0 turns a limit off.

| Setting (`Crawler:`) | Default | Covers |
|---|---|---|
| `ConnectTimeoutSeconds` | 5 | DNS resolution and connect, in `SafeConnect` (also `SocketsHttpHandler.ConnectTimeout`) |
| `FirstByteTimeoutSeconds` | 10 | From sending the request until the headers arrive |
| `RequestTimeoutSeconds` | 20 | A page request in total, body included |
| `RobotsTimeoutSeconds` | 10 | Each robots.txt attempt (https, then http), body included |
| `SitemapTimeoutSeconds` | 30 | Each sitemap file, body included |

The browser: `ChromeRendering:DefaultTimeoutMs` (default 30000) limits the navigation and every page call, and
`ChromeRendering:TotalTimeoutMs` (default 45000) caps the whole render. A Playwright timeout also gives status 0, `timeout`.

Decisions: [ADR 0003](adr/0003-timeouts-and-deadlines.md).

## Politeness (adaptive delay)

Politeness is pacing, not only waiting. `Hosts.CrawlDelayMs` (robots.txt Crawl-delay or the default 1 s) is the base delay and
is always the minimum. After each fetch `PolitenessPolicy` (pure, in `Core`) computes the next delay of the host, and the
frontier stores it in the same transaction that records the fetch (`UrlUpdate.Pacing`, `Hosts.NextAllowedAt = now + delay`, see
the in-flight hold below):

- **Slow host:** `AvgResponseMs` is a moving average (alpha 0.3) of the response times of fetches where the host answered
  (200 to 499, not 429). The delay is `max(base, 5 x AvgResponseMs)`.
- **Failures:** a timeout, network error, 5xx or 429 raises `ConsecutiveFailures` and doubles the delay for each failure in a row
  (2^n, n at most 10). Any answer resets the count to 0. The average does not change on a failure.
- **Cap:** the adaptive part is capped by `Crawler:MaxAdaptiveDelaySeconds` (default 60). The base delay stays the minimum, even
  when it is above the cap.
- **429 and 503:** with `Retry-After` (seconds or an HTTP date) the host waits at least that long, at most 1 hour, and the cap
  above does not apply. Without it the delay backs off from the base delay, 2^n, at most 10 minutes.
- **No change:** a blocked address and a robots.txt disallow touch neither `AvgResponseMs` nor `ConsecutiveFailures`.
- **Visibility:** `Hosts.LastDelayMs` keeps the delay that was applied. The job progress reports `max(CrawlDelayMs, LastDelayMs)`
  as `crawlDelayMs`, and the `FetchCompleted` event reports it as `delayMs`. `delaySource` still says `robots.txt` or `default`:
  it names the source of the base delay.

**One request in flight per host.** A claim does not book only the base delay. It holds the host for the longest possible fetch:
`max(Crawler:RequestTimeoutSeconds, ChromeRendering:TotalTimeoutMs / 1000) + Crawler:InFlightMarginSeconds` (default margin 5 s,
at most the 5 minute URL lease; `InFlightHold` computes it). A slow server therefore never gets a second concurrent request, even
when the response takes longer than the delay. Completing the URL, with any outcome (success, failure, timeout, redirect, robots
disallow, blocked address, retry later), releases the hold in the same transaction: `NextAllowedAt` becomes the completion time
plus the delay that applies (the adaptive delay, a Retry-After, or the base delay when the pacing is unchanged). It replaces the
hold, but it never lowers a later wait that was booked on purpose. If a worker dies, the hold expires by itself and the URL lease
recovers the URL. While a URL is in flight, the job progress reports `nextRequestAt` as null instead of the hold.

The URL itself keeps its retry rules (`OutcomeClassifier`, `BackoffPolicy`); the host pacing only decides when the host is
claimed again. `Crawler:MaxAdaptiveDelaySeconds` and `Crawler:InFlightMarginSeconds` are the settings of this section.

Decisions: [ADR 0004](adr/0004-frontier-pacing-and-politeness.md).

## Crawl directives (what sites allow)

Everything a site says about how it may be crawled is decided in `src/SamoBot.Core/Directives`. The parser and the fetchers
only extract raw data (meta tags, `<base href>`, canonical, `rel` values, and the response headers the directives ask for);
the directives interpret it and produce a `PagePolicy` per page, which is stored with the parsed page and enforced downstream.

| Signal | Where it is handled | Effect |
|---|---|---|
| `<meta name="robots">`, `<meta name="samobot">` (any case) | `Sources/MetaRobotsTagSource` | directives for the rules |
| `X-Robots-Tag` header, with `samobot:` / other-crawler scoping | `Sources/XRobotsTagSource` | directives for the rules |
| `nofollow`, `none` | `Rules/NoFollowRule` | no links of the page are queued |
| `noindex`, `none` | `Rules/NoIndexRule` | kept out of search, removed if indexed before |
| `nosnippet`, `max-snippet: N` | `Rules/NoSnippetRule`, `Rules/MaxSnippetRule` | search snippet hidden or cut to N characters |
| `noarchive` | `Rules/NoArchiveRule` | `GET /pages/{id}/content` answers `403` |
| `unavailable_after: DATE` | `Rules/UnavailableAfterRule` | hidden from search from that time (search filter on `indexUntil`) |
| canonical naming another page | `Canonical/*`, `Rules/NonCanonicalPageRule`, `Links/CanonicalTargetSource` | the copy is not indexed; the canonical URL is queued |
| `rel="nofollow"` / `"sponsored"` / `"ugc"` on a link | `Links/RelLinkRule` | that link is stored but not queued |
| `<meta http-equiv="refresh" content="N; url=...">` | `Links/MetaRefreshSource` | target queued like a redirect (same depth, even with nofollow) |
| `<base href>` | `PageContextFactory` | relative links, canonical and refresh targets resolve against it |

The strictest value wins when sources disagree. **To change behaviour, add or remove a class in `AddCrawlDirectives`**
(`DirectivesServiceCollectionExtensions.cs`); nothing else changes:

- a new place directives come from: implement `IRobotsDirectiveSource` (and `IResponseHeaderConsumer` if it reads a header:
  the fetchers then keep that header automatically);
- a new directive or signal: implement `IPagePolicyRule`, or derive from `DirectiveRule("name")`;
- a new per-link rule: implement `ILinkFollowRule`; which `rel` values block is an argument of `RelLinkRule`;
- a new redirect-like target: implement `IRedirectSource`; a new canonical source: implement `ICanonicalSource`.

A new kind of decision (a new `PagePolicy` property) also needs the place that enforces it, such as the indexer or the API.

Decisions: [ADR 0005](adr/0005-crawl-directives.md).

## Design decisions

The reasons behind the code are in [the ADRs](adr/README.md). The code has no comments (ADR 0001).

- PostgreSQL replaces RabbitMQ, Redis and Neo4j. One system holds all state, and one transaction records each result.
- `OutcomeClassifier` is the only place that maps a status code to a result. Each `CrawlOutcome` knows its new URL state.
- `PollingWorker<THandler>` is the only place in the worker that catches `Exception`.
- The host key is the URI authority (host plus a non-default port).
- Time comes from `TimeProvider`, also inside the SQL, so tests can move the clock.

## Limits (the build enforces them)

| Rule | Limit | Analyzer rule |
|---|---|---|
| Cyclomatic complexity of a method | 8 | CA1502 |
| Cognitive complexity of a method | 10 | S3776 |
| Lines in a method | 30 | S138 |
| Parameters of a method or constructor | 6 | S107 |
| Nesting depth of control blocks | 2 | S134 |
| Lines in a file | 300 | S104 |
| Braces on `if`, `for`, `foreach`, `while` | always | IDE0011 |
| Comments | none; `comments-baseline.txt` may only shrink | SB0001, SB0002 (`src/SamoBot.Analyzers`) |

`#pragma warning disable` and `[SuppressMessage]` are not permitted.

### Frontend limits

`src/frontend/eslint.config.js` applies the same limits to TypeScript. `npm run lint` fails on any of them.

| Rule | Limit | ESLint rule |
|---|---|---|
| Cyclomatic complexity of a function | 8 | `complexity` |
| Cognitive complexity of a function | 10 | `sonarjs/cognitive-complexity` |
| Lines in a function | 30 in `.ts`, 60 in `.tsx` (a component holds its markup) | `max-lines-per-function` |
| Parameters of a function | 6 | `max-params` |
| Nesting depth of control blocks | 2 | `max-depth` |
| Lines in a file | 300 | `max-lines` |
| Braces on `if`, `for`, `while` | always | `curly` |
| Comments | none; `eslint-suppressions.json` may only shrink (`npm run lint:baseline` prunes it) | `local/no-comments` |

`eslint-disable` comments and `@ts-ignore` are not permitted (`noInlineConfig`).
