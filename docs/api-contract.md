# Dashboard API contract

Source of truth for shapes: `src/frontend/src/types/api.ts`. The C# records in `SamoBot.Api/Contracts` must serialize to exactly those shapes (camelCase, enums as strings, ISO 8601 dates). Change a shape only through the lead.

Auth: a cookie session (`POST /api/v1/auth/google`). The `X-Api-Key` scheme keeps working for the MCP server and scripts. Every endpoint below needs either. Errors: `400` with `ProblemDetails`, `401` when not signed in, `404` when missing.

Transport notes:
- Sessions last at most 24 hours from sign-in and the account is checked against the allowlist on every request, so removing an account signs it out on the next call (`401`).
- Rate limit: 300 requests per minute per signed-in user (per client address when anonymous), configurable with `RateLimiting:PermitsPerMinute`. Over the limit the answer is `429` with `Retry-After: 60`. The SignalR hub is not limited.
- Every cookie-authenticated `POST`, `PUT`, `PATCH` and `DELETE` needs `X-Requested-With: samobot`. That includes the SignalR negotiate request: the client sends the header through the `headers` option of `withUrl`.
- The hub (`/hubs/*`) and `POST /api/v1/auth/google` check the `Origin` header when a browser sends one. It must be the API's own origin or one of `Cors:Origins`, otherwise `403`. Calls without `Origin` (scripts, the MCP server) pass.
- Cross-origin browsers can read the response headers `X-Content-Truncated` and `X-Content-Stored-Bytes` (exposed by CORS).

| Method and path | Request | Response |
|---|---|---|
| `POST /api/v1/auth/google` | `GoogleSignInRequest` | `204` and `Set-Cookie`; `403` if the account is not allowed |
| `POST /api/v1/auth/logout` | | `204` |
| `GET /api/v1/me` | | `Me` |
| `GET /api/v1/hosts?query&sort=host\|pages\|failed\|lastCrawl&limit&offset` | | `SitesResponse`; `offset` 0 to 100000 and `query` up to 200 characters, otherwise `400` |
| `GET /api/v1/hosts/{host}` | | `SiteDetail`; `400` when `{host}` is not a plain host (optionally `:port`) |
| `GET /api/v1/hosts/{host}/pages?query&state=all\|ok\|redirect\|error&limit&offset` | | `Page<PageSummary>`; same caps as the hosts list; `400` for an invalid `{host}` |
| `GET /api/v1/hosts/{host}/robots` | | `RobotsDto` (exists today as `RobotsResponse`; same fields) |
| `GET /api/v1/hosts/{host}/sitemaps?limit&offset` | | `SitemapDto` |
| `POST /api/v1/hosts/explain` | `{ url }` | `ExplainDto` |
| `GET /api/v1/pages/{id}` | | `PageDetail` |
| `GET /api/v1/pages/{id}/links?direction=out\|in&limit&offset` | | `Page<PageLinkDto>` = `{ total, items: [{ url, text, internal, noFollow, pageId, statusCode }] }`; `direction` defaults to `out`; `limit` 1 to 200 (default 50), `offset` 0 to 100000; `400` for a bad direction or offset, `404` for an unknown page |
| `GET /api/v1/pages/{id}/content` | | raw stored body, `text/plain`, size-capped (`X-Content-Truncated`), with the size of the whole stored body in `X-Content-Stored-Bytes`, `404` if not stored, `403` (problem details) when the page's policy forbids a stored copy (`noarchive`); sent with `X-Content-Type-Options: nosniff`, `Content-Security-Policy: default-src 'none'; sandbox` and an inline `Content-Disposition` |
| `GET /api/v1/pages/{id}/markdown` | | the stored body converted on demand to clean Markdown (headings, text, lists, tables, links, images; no scripts, styles or navigation; relative URLs made absolute; a body stored as Markdown or plain text is returned unchanged), `text/markdown; charset=utf-8`; `404` if not stored, `403` (problem details) for `noarchive`; same `X-Content-Truncated` (true when the source HTML was cut at the cap), `nosniff`, CSP and inline `Content-Disposition` (`page-{id}.md`) headers as `/content` |
| `GET /api/v1/search?q&host&limit&offset` | | `SearchResponse` (one document per URL, in the Meilisearch index `pages`; the old `parsed_documents` index can be deleted with `curl -X DELETE -H "Authorization: Bearer $MEILI_MASTER_KEY" http://localhost:7700/indexes/parsed_documents`) |
| `GET /api/v1/crawl-jobs?limit` | | `CrawlJobDto[]` |
| `GET /api/v1/crawl-jobs/{id}` | | `CrawlJobDto` |
| `POST /api/v1/crawl-jobs` | `CreateCrawlJobRequest` | `201 CrawlJobDto` |
| `POST /api/v1/crawl-jobs/{id}/actions` | `{ action: CrawlJobAction }` (`start`, `pause`, `cancel`, `recrawl`, `retry-failed`, `reprocess`) | `204`; `409` with the reason when the action is not allowed. `recrawl` queues every link of the job again, `retry-failed` only the failed and retrying ones; both make a finished job run again. `reprocess` is allowed only for a `Completed` job: the job is `Running` again, URLs whose body was skipped for a type that is stored now are fetched again, and the stored pages are parsed and indexed again (nothing stored is fetched again) |
| `GET /api/v1/crawl-jobs/{id}/links?filter=all\|problems\|notIndexed\|pending\|indexed&query&limit&offset` | | `Page<JobLinkDto>`: each link with its `state`, a `reason` sentence |
| `POST /api/v1/crawl-jobs/{id}/links/{linkId}/recrawl` | | `204`; `409` when the link is in flight, not part of the job, or the job was cancelled |
| `GET /api/v1/crawl-jobs/{id}/progress` | | `CrawlProgress` |
| `GET /api/v1/crawl-jobs/{id}/events?after&limit` | | `CrawlEvent[]` with `id`, ascending. `limit` defaults to 100 (1..500). Without `after`: the newest `limit` events. With `after` (negative counts as 0): events with `id > after`, at most `limit`; the next cursor is the largest `id` returned. |
| `GET /api/v1/analytics/overview?range` | | `AnalyticsOverview` |
| `GET /api/v1/analytics/hosts?range` | | `HostAnalytics[]` |
| `GET /api/v1/analytics/status-codes?range&host` | | `StatusClassCounts` |
| `GET /api/v1/analytics/content-types?range&host` | | `ContentTypeCount[]` |
| `GET /api/v1/analytics/timeseries?range&host&bucket=hour\|day` | | `TimeseriesPoint[]` |

Notes
- Percentiles use `percentile_cont` over `UrlFetches.ResponseTimeMs`. `requests` counts rows of `UrlFetches`; `pages` counts `DiscoveredUrls` rows with a fetch.
- A site's `failed` counts pages whose last fetch was 4xx/5xx or never completed, and disabled URLs with another failure reason; redirects, robots-disallowed and blocked-address URLs are not counted. The sites overview is cached for 30 seconds.
- A site's `status`: `Crawling` if a Running job has its host; `Paused` if its newest job is Paused; `Crawled` if it has fetches; else `Never`.
- robots.txt is always respected. `CreateCrawlJobRequest` has no switch for it, and a `respectRobots` field in a request is ignored.
- `CrawlJobDto.links` (`JobLinkSummary`) counts the links of the job by state and lists why the failed ones failed. A link's state: `Queued`, `Fetching`, `Retrying` (failed, tried again automatically), `Parsing`, `Indexing`, `Indexed`, `NotIndexed` (the site asked: noindex, canonical elsewhere, unavailable_after), `Skipped` (robots.txt, blocked address, redirect, not HTML, empty body) or `Failed`. A recrawled link reads as `Queued` until it is fetched again; its fetch history stays.
- Seed URL rules for `POST /crawl-jobs`: absolute `http` or `https` only; reject loopback, private, link-local and metadata addresses (SSRF).
- `snippet` is plain text plus `<mark>`/`</mark>`; the client must not use `dangerouslySetInnerHTML`.
- Page links (table `Links`, written by the parser in the same transaction that stores the page): every http and https link of a page, internal or external, in scope or not, deduplicated by normalized target (the first anchor wins), at most 2000 for each page; targets longer than 2048 characters are skipped. `PageDetail.parsed.linkCount` and `internalLinkCount` count those rows (internal = same host and port as the page). A re-parse replaces the links of the page. Pages parsed before this feature have no rows until they are parsed again, so their counts are 0 and they list no links; there is no backfill.
- `PageDetail.foundOn` is `{ pageId, url }` of the page with the earliest stored link to this page (same job, not the page itself), or `null` (seeds, or no link stored yet).
- `GET /pages/{id}/links`: `direction=out` lists the links on the page, ordered by their order of storage; `url` is the target, `text` the anchor (or `null`), `pageId` and `statusCode` describe the target when it is a known page of the same job (else `null`). `direction=in` lists the pages that link to this page, earliest first: `url` is the linking page, `text` its anchor, `internal` whether it is on the same host, `pageId` and `statusCode` the linking page and its latest fetch status. `statusCode` is `null` when the page has no fetch yet.
- `CrawlProgress.delaySource` is `adaptive` when the delay applied after the last fetch is larger than the host's base delay, else `robots.txt` or `default`.
