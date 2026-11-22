# 0016. Path rules replace the page limit, render mode and browser gate

Status: Accepted

## Context

A crawl used to start at once from a URL, "max depth", "max pages" and a "Render JavaScript" switch. Nobody can guess a page
limit, a site that needs JavaScript was stored almost empty, and a job that asked for the browser silently used HTTP when the
browser service was down. The dashboard now checks the site first, shows its sitemap as a tree and starts the crawl from that plan.

## Decision

**Site check (`SiteCheckService`, `POST /api/v1/site-checks`).** Nothing is crawled and no job exists.
- `StartPageResolver` follows at most 5 redirects, by hand, through `FetcherResolver` (so `SafeConnect` and the deadlines apply)
  and asks robots.txt before every hop. The crawl then uses the final host. This is the only place a host change is allowed; a
  crawl never leaves its one host.
- The sitemap is never walked inside the request. `SitemapPlanner` reads `ISitemapPlanReader.Status`; `SitemapPlanRules.Decide`
  returns Ready or None for a walk that finished within `SitemapBatchHandler.RefreshAfter`, Pending or Failed for a queued one, and
  otherwise asks `ISitemapStore.Enqueue`. The worker does the walk with `SitemapRefresher`, so the robots and sitemap logic exist
  once. The client polls `GET /hosts/{host}/plan/sitemap` and gives up waiting after 3 minutes (the plan then has no sitemap).
- `JsDetectionService` fetches the start page over HTTP, parses it and asks `JsDetector` (the only place with thresholds).
  `ParsedPage.Shell` (`PageShell`) holds the parse-time signals: visible text, links, executable scripts, an empty app container
  (`#root`, `#app`, `#__next`, `#___gatsby`, `#svelte`, `app-root`) and a noscript "enable JavaScript" notice. Only a suspect page,
  and only when the browser answers, is rendered once and compared (`NeedsJs`: clearly more text or links). A suspect page with no
  browser is `ProbablyNeeded`: not confirmed.

**Render mode (`RenderMode`, migration 028).** `Auto | Always | Never` replaces `CrawlJobs.UseJsRendering`. The migration maps
false to Never and true to Always, so old jobs behave as before; the column default and the API default for new jobs is Auto.
`CreateCrawlJobRequest.useJsRendering` is still read when `renderMode` is missing. `DiscoveredUrls.UseJsRendering` stays the
per-URL decision. It is set when the URL is inserted (`PathRuleSql.UseBrowser`: Always, or Auto when the check found that the site
needs JavaScript, `CrawlJobs.JsNeeded`). In Auto, `ParsePageHandler` sends an HTTP page that parses as an empty shell back to the
frontier with `IParseBacklog.Escalate`; the browser fetches it next. The decision is per job and per URL. It is not stored per host
(`Hosts`); that is left out on purpose.

**Browser availability and the gate.**
- `IBrowserProbe` is a real reachability probe (`CdpBrowserProbe`: `GET {endpoint}/json/version`, 3 s). `BrowserAvailability` keeps
  the answer for 5 s (`Current`); `Refresh` always asks (the gate and the "Check again" button).
- `BrowserGate.Ensure` refuses to create and start, or to start, a job whose `NeedsBrowser` is true (Always, or Auto with
  `JsNeeded`) while the probe fails. The error is `BrowserUnavailableError`; the API answers 409 with the problem-details member
  `code: "browser-unavailable"`. Mode Never always starts. A plain create without start is not gated. The server trusts the
  `jsNeeded` flag the dashboard sends from its own check; a client that lies only changes how its own crawl is fetched.
- The worker claims a URL that needs the browser only while the browser is up (`IUrlFrontier.Claim(..., browserAvailable)`). In
  Auto on a site that does not need JavaScript, an escalated page therefore waits and the rest goes on. `HttpFetcher` no longer
  falls back to HTTP for such a URL.
- `BrowserGuardHandler` pauses running jobs that need the browser with `PauseReason = browser-unavailable` while it is down and
  resumes exactly those when it is back (`IBrowserPauseStore`). A job the user paused is never resumed by it.

**Path rules (`PathRules`, `JobScope`, `PathRuleSql`).**
- A rule is `{ kind: Include|Exclude, prefix, exact }`. A folder rule ends with a slash and covers the folder and everything
  below it (`/blog/2016` counts as inside `/blog/2016/`). An exact rule covers one path. The query is not part of the path. Paths
  are matched with case.
- The best matching rule wins: exact before folder, then the longer prefix, then Include over Exclude. No match means included.
  So "untick `/blog/`, tick `/blog/2026/`" works, and a page found later follows its folder.
- The start page is pinned as an exact Include (`PinnedStartPages`), so it beats every other rule. robots.txt still wins at fetch
  time (`RobotsGate`).
- The rules are enforced in two places that must agree: `JobScope` (C#, for links and redirects found by the parser) and
  `NewUrlInserter` (SQL, for every insert including seeds and HTTP redirects). The SQL reads `CrawlJobs.PathRules` as JSON and
  uses the same order (`PathRuleSql.Allows`). `PathRulesTests` and `PostgresUrlFrontierTests` pin the two to the same cases. The
  dashboard has a third copy for the tick state (`path-rules.ts`, same tests).
- `MaxUrls` and `MaxDepth` stay as columns and API fields for old rows. The new dashboard never sets them.
- `PUT /crawl-jobs/{id}/path-rules` saves the rules (`SavePathRules`, a narrow UPDATE so it cannot overwrite a status) and
  applies them to the queue (`IPathRuleApplier`): URLs that were never fetched in an unticked folder become `Disabled` with the
  fail reason `unselected`; a re-ticked folder puts them back to `Idle`. Pages already fetched are kept. `unselected` counts as
  skipped, not failed, in the progress, link, site and recrawl SQL.

**Sitemap seeds and the review pause.**
- `SeedFromSitemap` jobs insert the sitemap URLs that the rules allow at depth 0 with one `INSERT ... SELECT`
  (`PostgresSitemapSeeder`) on the first start, after the start page. `FollowLinkedPages` (default on) keeps link following; when
  off, `JobScope.Filter` queues no link (redirects still follow). Without a sitemap it is always on.
- A job without a sitemap may pause itself after `ReviewAfterPages` fetched pages (`ReviewGuardHandler`, `PauseReason = review`).
  Starting it again clears the review. `Paused` plus `PauseReason` is used instead of a new status.

**Trees (`PathTreeBuilder`, `FolderGroupSql`).** The server counts and groups; the browser never gets the URLs.
- One SQL statement groups the paths below a parent by their next two segments. `PathTreeBuilder` turns the groups into at most
  two levels (`depth` 1 or 2) and keeps the 40 largest children, with a `Rest` node for the others. The client opens deeper
  folders one request at a time.
- `GET /hosts/{host}/plan/tree` reads `SitemapUrls` (pages, last-modified range, robots-blocked flag). `GET /crawl-jobs/{id}/tree`
  reads `DiscoveredUrls` (done, failed, skipped, stopped, fetching, queued). Both take the path from the URL with a regular
  expression, so a sitemap of 500,000 URLs is one scan per request. A `Path` column with an index is the next step if that is too
  slow.
- The dashboard computes tick state, partly-ticked folders and the "ticked of total" summary from the loaded nodes and the rules
  (`path-rules.ts`, `plan-totals.ts`). A rule can only be made on a node that was loaded, so the count is exact.

## Not done

- The runaway-folder guard (a folder whose discovered count keeps growing far beyond the selection) is not built. The tree and
  the path rules endpoint are what it needs: it would compare `GET tree` counts with the sitemap counts, pause with a
  `PauseReason` and let the user untick the folder.
- Render mode is not remembered per host.
- Search in the tree covers the folders that are loaded; typing a query loads the open levels (at most 30 folders per action).
- The tree has no "not in the sitemap" mark for pages found by following links.

## Consequences

- A migration drops `CrawlJobs.UseJsRendering`. Run migrations before the new API and worker start.
- `CrawlJobDto` has `renderMode`, `jsNeeded`, `pathRules`, `seedFromSitemap`, `followLinkedPages`, `reviewAfterPages`,
  `pauseReason` and `pauseDetail`; `useJsRendering` stays as a derived value.
- A site check makes outbound requests (up to 5 hops, the start page, and one browser render), limited only by the general rate
  limit.
