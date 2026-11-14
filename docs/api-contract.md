# Dashboard API contract

Source of truth for shapes: `src/frontend/src/types/api.ts`. The C# records in `SamoBot.Api/Contracts` must serialize to exactly those shapes (camelCase, enums as strings, ISO 8601 dates). Change a shape only through the lead.

Auth: a cookie session (`POST /api/v1/auth/google`). The `X-Api-Key` scheme keeps working for the MCP server and scripts. Every endpoint below needs either. Errors: `400` with `ProblemDetails`, `401` when not signed in, `404` when missing.

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
| `GET /api/v1/pages/{id}/content` | | raw stored body, `text/plain`, size-capped, `404` if not stored; sent with `X-Content-Type-Options: nosniff`, `Content-Security-Policy: default-src 'none'; sandbox` and an inline `Content-Disposition` |
| `GET /api/v1/search?q&host&limit&offset` | | `SearchResponse` |
| `GET /api/v1/crawl-jobs?limit` | | `CrawlJobDto[]` |
| `GET /api/v1/crawl-jobs/{id}` | | `CrawlJobDto` |
| `POST /api/v1/crawl-jobs` | `CreateCrawlJobRequest` | `201 CrawlJobDto` |
| `POST /api/v1/crawl-jobs/{id}/actions` | `{ action: CrawlJobAction }` | `204` |
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
- Seed URL rules for `POST /crawl-jobs`: absolute `http` or `https` only; reject loopback, private, link-local and metadata addresses (SSRF).
- `snippet` is plain text plus `<mark>`/`</mark>`; the client must not use `dangerouslySetInnerHTML`.
