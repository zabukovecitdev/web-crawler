# SamoBot

A polite web crawler with a dashboard. PostgreSQL is the only queue.

```
                +-----------------------------+
 Dashboard ---> |  Api   (REST + SignalR)     |<--- LISTEN crawl_events
                +--------------+--------------+
                               |
                               v
                       +---------------+
                       |  PostgreSQL   |  frontier, fetches, pages, jobs, events
                       +-------+-------+
                               ^
        claim with a lease     |     record one result
   +---------------------------+--------------------------+
   |  Worker                                              |
   |  crawl loop --> RustFS (HTML, Markdown, plain text)  |
   |  parse loop <-- RustFS, adds links to the frontier   |
   |  index loop --> Meilisearch                          |
   |  completion loop (marks finished jobs)               |
   +------------------------------------------------------+
```

Projects: `Core` (domain, use cases, port interfaces, no I/O packages), `Adapters` (PostgreSQL, HTTP,
browser, S3 storage, Meilisearch, HTML), `Worker`, `Api`, `Migrations`, `SamoBot.Mcp` (search server for AI tools).
The compiler enforces the dependency rule: `Core` references no project.

## How to run

```bash
cp .env.example .env
openssl rand -hex 32   # put the output in .env as DASHBOARD_API_KEY
docker compose up --build
```

This starts PostgreSQL, RustFS (S3 storage, console at http://localhost:9001/rustfs/console/), Meilisearch, the migrations, the worker, the API (port 5080) and the dashboard (http://localhost:8081).
Send the key in the header `X-Api-Key` (MCP server and scripts). The dashboard is in `src/frontend/`. The page screen has a Markdown tab: `GET /api/v1/pages/{id}/markdown` turns the stored HTML into clean Markdown (headings, text, lists, tables, links, images) on demand, for reading or as input for an LLM.

- **API key:** `DASHBOARD_API_KEY` is required. In Production (the Docker images) the API refuses to start when the key is shorter than 32 characters or a placeholder such as `change-me`; an existing deployment with a short key must be changed.
- **Ports:** compose binds every published port (PostgreSQL, RustFS, Meilisearch, the API) to `127.0.0.1`. The database, storage and search credentials default to development values; set `POSTGRES_PASSWORD`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` and `MEILI_MASTER_KEY` in `.env` before publishing on another interface, and then only behind a firewall.
- **Cookie:** compose serves plain http on localhost, so `AUTH_ALLOW_INSECURE_COOKIE` defaults to `true` (the session cookie is sent without `Secure`). Set it to `false` behind HTTPS. Outside Development without this flag the cookie is always `Secure`.
- **Allowlist changes:** the allowlist is checked on every request, so removing an account signs it out on its next call. `appsettings.json` changes are picked up at once; environment variables (compose) are read once, so restart the `api` container after editing `AUTH_ALLOWED_EMAILS` or `AUTH_ALLOWED_DOMAINS`. A session also ends 24 hours after sign-in.
- **Swagger** is served only in Development (`dotnet run`).
- **Rate limits** (details in "Rate limiting" below): nginx limits per client address, the API adds a backstop per user; over a limit the answer is `429` with `Retry-After`.
- **Dashboard container:** nginx serves the built dashboard and forwards `/api` and `/hubs` to the API, so the browser sees one origin and needs no CORS. Vite builds `GOOGLE_CLIENT_ID` into the bundle: rebuild the image (`docker compose up --build frontend`) after you change it, and add `http://localhost:8081` to the authorised JavaScript origins of the Google client. The container has no API-key sign-in; use the Vite dev server for that (see below).
- **Dashboard in dev:** the API listens on http://localhost:5080 with both `dotnet run` and compose. With `dotnet run` it allows the origins in `appsettings.Development.json`; with compose it allows `CORS_ORIGIN`.

### JavaScript rendering

Pages that need JavaScript can be rendered with headless Chrome. It is off by default:

```bash
CHROME_RENDERING_ENABLED=true docker compose --profile js up --build
```

The `chrome` service sits on two networks of its own: `browser` (internal, shared only with the worker) and `chrome_egress` (a bridge with the fixed subnet `172.31.250.0/24`). It is not on the default network, so Chrome cannot reach PostgreSQL, RustFS, Meilisearch or the API. The worker reaches it at `http://172.31.251.10:9222` (an IP, because the DevTools endpoint rejects other `Host` headers).

Chrome resolves host names itself, so the address checks of the crawler cannot close a DNS rebinding window completely. The network is the control: drop new connections from the egress subnet to private and special ranges on the Docker host.

```bash
iptables -I DOCKER-USER -s 172.31.250.0/24 -m conntrack --ctstate NEW \
  -d 10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,169.254.0.0/16,100.64.0.0/10,127.0.0.0/8 -j DROP
```

On a cloud deployment use the provider's egress controls (security group or network policy) for the same ranges, including the metadata address 169.254.169.254. This setup has not been verified end to end here (no Docker available); run `docker compose --profile js up` once and crawl a JavaScript page before relying on it.

### Dashboard sign-in (Google)

The dashboard signs in with Google and the API issues a cookie session (`samobot.session`: HttpOnly, SameSite=Lax, 8 hours sliding and at most 24 hours from sign-in, Secure outside Development unless `AUTH_ALLOW_INSECURE_COOKIE=true`).
Set these in `.env`:

| Variable | Meaning |
| --- | --- |
| `GOOGLE_CLIENT_ID` | OAuth client ID (Web application). The dashboard origin must be an authorised JavaScript origin. |
| `AUTH_ALLOWED_EMAILS` | Comma-separated emails that may sign in. |
| `AUTH_ALLOWED_DOMAINS` | Comma-separated domains (matched against the verified `hd` claim or the email domain). |
| `CORS_ORIGIN` | Dashboard origin, default `http://localhost:5173`. |

Both lists empty means nobody can sign in (the API logs a warning at startup; the API key still works). Config keys: `Google:ClientId`, `Auth:AllowedEmails`, `Auth:AllowedDomains`.
`http://localhost:5173` talking to `http://localhost:5080` is same-site (ports do not matter), so the Lax cookie works in dev over plain http.
Frontend contract: `fetch(..., { credentials: 'include' })`; send `X-Requested-With: samobot` on every POST/PUT/PATCH/DELETE (cookie sessions only; `/auth/google` is exempt); SignalR uses the same cookie. Unauthenticated calls return 401, never a redirect.

### Dashboard without Google sign-in (local development)

Until a Google client ID is set, the Vite dev server can sign every request in with the API key. The key stays in
the dev server (no `VITE_` prefix), so it is never sent to the browser or built into `dist/`. Do not use this outside
your own machine: anyone who can open the dev server acts as the API key user.

```bash
docker compose up --build                 # API on http://localhost:5080
cd src/frontend
cp .env.example .env.local
# in .env.local: VITE_API_BASE=   (empty)   DEV_API_TARGET=http://localhost:5080   DEV_API_KEY=<DASHBOARD_API_KEY from ../../.env>
npm ci && npm run dev                     # http://localhost:5173
```

The dashboard then shows the user "Dashboard". Signing out only shows the sign-in page; reload to come back.
Once `GOOGLE_CLIENT_ID` and an allowlist are set, remove `DEV_API_KEY` and set `VITE_API_BASE=http://localhost:5080`.

Tests:

```bash
dotnet build SamoBot.sln -warnaserror
dotnet test src/SamoBot.Tests --filter "Category!=Integration"
dotnet test src/SamoBot.Tests --filter "Category=Integration"   # needs Docker
```

## Rate limiting

Two layers, so that no single mistake leaves the login or any other public endpoint open. Both answer `429` with `Retry-After`.

| Layer | Where | Key | What it limits |
|---|---|---|---|
| Edge (leaky bucket, `limit_req`) | `src/frontend/nginx-limits.conf` (zones) and `nginx.conf` (which location uses which zone) | client address | `/api/`: 10 requests/s, burst 50. `POST /api/v1/auth/google`: 6/min, burst 3 (plus the `/api/` limit). `/hubs/`: 1/s, burst 10, at most 20 open connections |
| API (token bucket) | `Hosting/RateLimitingExtensions.cs`, section `RateLimiting` | signed-in user, otherwise client address | `PermitsPerMinute` 300 per user (cookie or API key). `AnonymousPermitsPerMinute` 120 per address, `SignInPermitsPerMinute` 10 on sign-in, `ApiKeyFailuresPerMinute` 10 for an `X-Api-Key` that does not match. The hub is exempt for a signed-in user. |

A bucket holds a burst (a fifth of the per-minute rate, at least 3) and refills steadily, so there is no window edge where a caller can double its rate. The API limiter runs after authentication and before authorization, so unauthenticated calls to protected endpoints and wrong API keys are counted. An IPv6 client is limited as a whole /64.

Both layers must see the real client address, not the proxy's. nginx trusts `X-Forwarded-For` only from private networks (`set_real_ip_from` in `nginx-limits.conf`) and passes the address on; the API trusts it only from `ForwardedHeaders:KnownProxies` and `ForwardedHeaders:KnownNetworks` (compose sets the private ranges). A header from any other peer is ignored. For that reason never publish the dashboard or API container port to the internet directly: put a proxy in front (next section). Behind a cloud load balancer or CDN, add its address ranges to both settings. nginx keeps its counters in memory, so the limits are per nginx instance.

## Production

`compose.prod.yaml` adds a TLS edge to the stack on a single host. Only Caddy publishes ports (80 and 443) and gets a Let's Encrypt certificate by itself; the dashboard, API and backing services are reachable only on the compose network, so every request passes the rate limits.

```bash
# .env: DOMAIN, DASHBOARD_API_KEY, GOOGLE_CLIENT_ID, AUTH_ALLOWED_EMAILS or AUTH_ALLOWED_DOMAINS,
#       POSTGRES_PASSWORD, S3_ACCESS_KEY, S3_SECRET_KEY, MEILI_MASTER_KEY (16+ bytes). Compose stops when one is missing.
docker compose -f compose.yaml -f compose.prod.yaml up -d --build
```

The DNS name `DOMAIN` must point at the host, and `https://DOMAIN` must be an authorised JavaScript origin of the Google client. The override also sets `AUTH_ALLOW_INSECURE_COOKIE` to false and CORS to `https://DOMAIN`. It needs Docker Compose 2.24.4 or newer. CI builds and signs the four images (`samobot-api`, `-worker`, `-migrations`, `-frontend`) in `ghcr.io`; the frontend image needs the repository variable `GOOGLE_CLIENT_ID` because Vite builds it into the bundle. To run the published images instead of building, point the `image:` of each service at them. Back up the `postgres_data` volume and the RustFS bucket; neither is backed up for you.

## The claim pattern

Every stage does the same three steps: claim rows with a lease, do the work, record one result.

- **Crawl:** a claim takes at most one due URL for each host and locks the host row. The host row also holds the
  delay until the next request, so politeness needs no extra service.
- **Parse:** a claim leases stored fetches with `FOR UPDATE SKIP LOCKED`. One transaction stores the page, replaces the links of the page in `Links` (all of them, for the link lists of the dashboard),
  adds the in-scope links and marks the fetch parsed.
- **Index:** pages with no `IndexedAt` go to Meilisearch in one batch. Indexing is idempotent.

- **Stored formats:** HTML, Markdown and plain text (`StoredMediaTypes`). Page requests send `Accept` with HTML first,
  then Markdown, then plain text. Markdown and plain text are converted to HTML at parse time. Other types are fetched
  and skipped.
- **Recrawl:** the job action `recrawl` runs a completed job again with the current code. It fetches again the URLs
  whose body was skipped for a type that is stored now, and parses (and so indexes) the stored pages again.

A worker that dies leaves a lease. When the lease ends, the next claim takes the row again.
A job is complete when no URL is in flight, no URL waits for its first fetch or a retry, and no page waits for the parser.

## Network safety (SSRF)

Seed URLs are checked as text (`SeedUrlValidator`). The real defence runs at connect time, because a public name can
resolve to a private address (DNS rebinding) and a redirect can point at one.

- **Connect check:** the clients of the fetcher and of robots.txt use `SafeConnect`, a `SocketsHttpHandler.ConnectCallback`.
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
  private ranges.
- **Configuration:** `Crawler:AllowPrivateAddresses` (default false; development and tests only, logs a startup warning),
  `Crawler:UseProxy` (default false; a proxy would hide the target from the connect check),
  `Crawler:DnsCacheMinutes` (default 60).

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

## Politeness (adaptive delay)

Politeness is pacing, not only waiting. `Hosts.CrawlDelayMs` (robots.txt Crawl-delay or the default 1 s) is the base delay and
is always the minimum. After each fetch `PolitenessPolicy` (pure, in `Core`) computes the next delay of the host, and the
frontier stores it in the same transaction that records the fetch (`UrlUpdate.Pacing`, `Hosts.NextAllowedAt = now + delay`, see the in-flight hold below):

- **Slow host:** `AvgResponseMs` is a moving average (alpha 0.3) of the response times of fetches where the host answered
  (200 to 499, not 429). The delay is `max(base, 5 x AvgResponseMs)`.
- **Failures:** a timeout, network error, 5xx or 429 raises `ConsecutiveFailures` and doubles the delay for each failure in a row
  (2^n, n at most 10). Any answer resets the count to 0. The average does not change on a failure.
- **Cap:** the adaptive part is capped by `Crawler:MaxAdaptiveDelaySeconds` (default 60). The base delay stays the minimum, even when it is above the cap.
- **429 and 503:** with `Retry-After` (seconds or an HTTP date) the host waits at least that long, at most 1 hour, and the cap above does not apply.
  Without it the delay backs off from the base delay, 2^n, at most 10 minutes.
- **No change:** a blocked address and a robots.txt disallow touch neither `AvgResponseMs` nor `ConsecutiveFailures`.
- **Visibility:** `Hosts.LastDelayMs` keeps the delay that was applied. The job progress reports `max(CrawlDelayMs, LastDelayMs)` as `crawlDelayMs`,
  and the `FetchCompleted` event reports it as `delayMs`. `delaySource` still says `robots.txt` or `default`: it names the source of the base delay.

**One request in flight per host.** A claim does not book only the base delay. It holds the host for the longest possible fetch:
`max(Crawler:RequestTimeoutSeconds, ChromeRendering:TotalTimeoutMs / 1000) + Crawler:InFlightMarginSeconds` (default margin 5 s, at most the
5 minute URL lease; `InFlightHold` computes it). A slow server therefore never gets a second concurrent request, even when the response takes
longer than the delay. Completing the URL, with any outcome (success, failure, timeout, redirect, robots disallow, blocked address, retry later),
releases the hold in the same transaction: `NextAllowedAt` becomes the completion time plus the delay that applies (the adaptive delay, a
Retry-After, or the base delay when the pacing is unchanged). It replaces the hold, but it never lowers a later wait that was booked on purpose.
If a worker dies, the hold expires by itself and the URL lease recovers the URL. While a URL is in flight, the job progress reports
`nextRequestAt` as null instead of the hold.

The URL itself keeps its retry rules (`OutcomeClassifier`, `BackoffPolicy`); the host pacing only decides when the host is claimed again.
`Crawler:MaxAdaptiveDelaySeconds` and `Crawler:InFlightMarginSeconds` are the settings of this section.

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

## Design decisions

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

`eslint-disable` comments and `@ts-ignore` are not permitted (`noInlineConfig`).

