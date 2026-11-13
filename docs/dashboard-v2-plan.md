# SamoBot dashboard v2 — implementation and orchestration plan

Status: **plan only. Nothing here is implemented yet.** Implementation starts after the design canvas is approved.

Design canvas: https://claude.ai/artifact/XLoJ81sBh8XN3scNPo572h
Design system: Zabukovec — https://claude.ai/artifact/2sFJdFsSq2K3FEHAkprNNg

## 1. What exists today (verified in the repo)

| Requirement | Today | Gap |
|---|---|---|
| Google sign-in | API key only (`ApiKeyAuthenticationHandler`, `X-Api-Key`; key is baked into the frontend via `VITE_API_KEY`) | Whole feature |
| List of hosts / sites | No endpoint. `Hosts` table holds only `Host`, `NextAllowedAt`, `CrawlDelayMs` | Read model + endpoint + screen |
| Pages per host, content, crawl metadata | `GET /crawl-jobs/{id}/pages` lists frontier URLs per *job*, no content, no fetch history | Host-scoped list, page detail, content endpoints |
| Search across sites | `ISearchQuery` (Meilisearch) is used only by `SamoBot.Mcp`. `SearchHit` has no host, URL or highlight | API endpoint, index fields, screen |
| Request a crawl by URL | `POST /crawl-jobs` + `POST /crawl-jobs/{id}/actions` exist | UI only (create + start in one step) |
| Live crawl status | SignalR hub `/hubs/crawl-job` forwards `CrawlEvent`. `FetchCompleted` payload is only `{url, statusCode, outcome}`. No "fetch started" event | Richer payload, progress snapshot, screen |
| Sitemap and robots.txt | robots: `GET /hosts/{host}/robots` works. sitemaps: endpoint is a stub that returns `[]` | Sitemap discovery + storage + endpoint |
| Analytics | Nothing | Aggregation queries + endpoints + screen |
| shadcn/ui | Hand-written `components/ui/*`, no `components.json`, Tailwind 3 with an empty theme | `shadcn init`, token mapping |

Constraints agents must respect:
- C# analyzer limits fail the build (method ≤ 30 lines, complexity ≤ 8, ≤ 6 parameters, file ≤ 300 lines, nesting ≤ 2, no pragma/suppress). `dotnet build SamoBot.sln -warnaserror` is the gate.
- `Core` references no project. New ports go in `Core/Ports`, SQL in `Adapters/Postgres`, table names only via `TableNames`.
- `docs/crawling-dashboard-plan.md` is stale (it describes Redis and services that no longer exist). Do not use it as a source.
- No shadcn skill is installed in this cloud session. Agents use the shadcn CLI (`npx shadcn@latest`) and its documented conventions unless the skill is added first.

## 2. Decisions (defaults; change before kickoff if you disagree)

1. **Auth flow.** Google Identity Services button in the SPA returns an ID token. `POST /api/v1/auth/google` validates it (issuer, audience = `Google:ClientId`, signature), checks an allowlist (`Auth:AllowedEmails` / `Auth:AllowedDomains`), then issues an HttpOnly, SameSite cookie session. Cookie auth works for REST and for SignalR without putting tokens in query strings; CORS already allows credentials. The API-key scheme stays for the MCP server and scripts, and `VITE_API_KEY` is removed from the frontend.
2. **"Site" = host** (URI authority, as the crawler already defines it). A site aggregates every job that touched that host.
3. **Request a crawl** = create a job with one seed URL and start it immediately. Depth, page limit, JS rendering and robots sit under "Options" with the current defaults.
4. **Live status** = snapshot + stream. `GET /crawl-jobs/{id}/progress` gives counters; SignalR events update them. Polling `/events?after=` is the fallback when the socket drops.
5. **Charts** = shadcn `chart` (Recharts). Colours come only from Zabukovec tokens.
6. **Server state** = TanStack Query (already installed). No global store.

## 3. Target API contract (Phase 0 freezes this)

Auth
- `POST /api/v1/auth/google` `{ credential }` → `204` + cookie · `POST /api/v1/auth/logout` · `GET /api/v1/me` → `{ userId, name, email, pictureUrl }`

Sites
- `GET /api/v1/hosts?query=&sort=&limit=&offset=` → `{ total, items: [{ host, pages, fetched, failed, lastCrawlAt, crawlDelayMs, avgResponseMs, activeJobId }] }`
- `GET /api/v1/hosts/{host}` → summary + status-code and content-type counts
- `GET /api/v1/hosts/{host}/pages?status=&query=&limit=&offset=` → `{ total, items: [{ id, url, title, statusCode, contentType, contentLength, responseTimeMs, depth, lastCrawlAt, parsed, indexed }] }`
- `GET /api/v1/hosts/{host}/robots` (exists) · `GET /api/v1/hosts/{host}/sitemaps` → `{ sitemaps: [{ url, fetchedAt, urlCount }], urls: [{ loc, lastmod, crawled }] , total }`

Pages
- `GET /api/v1/pages/{id}` → url, fetch history (`FetchRecord[]`), parsed document (title, description, language, canonical, headings, link/image counts, robots directives, Open Graph), body text
- `GET /api/v1/pages/{id}/content` → stored raw HTML (`text/plain`, never rendered as HTML in the dashboard)

Search
- `GET /api/v1/search?q=&host=&limit=&offset=` → `{ total, tookMs, hosts: [{ host, count }], items: [{ pageId, host, url, title, snippet (with highlight marks), language, fetchedAt }] }`

Crawl
- `POST /api/v1/crawl-jobs` (exists) gains `startImmediately`
- `GET /api/v1/crawl-jobs/{id}/progress` → `{ status, crawled, queued, inFlight, failed, currentUrl, crawlDelayMs, avgResponseMs, startedAt }`
- SignalR `CrawlEvent`: add `FetchStarted { url }`; `FetchCompleted` gains `responseTimeMs, contentType, contentLength, contentStatus (Stored | Skipped | Failed), delayMs, depth`

Analytics
- `GET /api/v1/analytics/overview` → hosts, pages, fetches, bytes, failure rate, p50/p95 response time
- `GET /api/v1/analytics/hosts?limit=` → per host: pages, fetches, p50, p95, avg, max, bytes, failure rate
- `GET /api/v1/analytics/status-codes?host=` · `GET /api/v1/analytics/content-types?host=` · `GET /api/v1/analytics/timeseries?host=&bucket=hour|day`

## 4. Orchestration

One **lead** (the main session) owns the contract, shared files, merging and verification. Workers are subagents, each in its own git worktree, each owning a disjoint set of paths. A worker never edits a path it does not own; it reports the need to the lead instead.

### Phase 0 — contract (lead, serial, blocks everything)
- Write `docs/api-contract.md` from section 3, `src/frontend/src/types/api.ts`, and JSON fixtures in `src/frontend/src/mocks/` (MSW handlers) taken from the design canvas data.
- Result: frontend and backend agents can work in parallel against the same shapes.

### Phase 1 — foundations (3 agents in parallel)
| Agent | Owns | Delivers |
|---|---|---|
| **F0 frontend foundation** | `src/frontend/` config, `src/index.css`, `components/ui/*`, `components/layout/*`, `lib/*`, `App.tsx`, `routes.tsx` | `shadcn init`; Zabukovec tokens as CSS variables (light + dark) mapped to shadcn variables; Geist fonts; shadcn components: button, input, card, badge, table, tabs, dialog, dropdown-menu, sidebar, skeleton, sonner, switch, tooltip, chart; app shell; `AuthProvider`, sign-in page, protected routes; `apiFetch` with cookies and 401 handling; one empty route module per feature; MSW wired for `npm run dev` |
| **B0 backend auth** | `SamoBot.Api/Authentication/*`, `Controllers/AuthController.cs`, `MeController.cs`, auth part of `Program.cs`, `appsettings`, `compose.yaml`, `.env.example` | Google ID-token validation, allowlist, cookie session, policy scheme (cookie or API key), hub auth, unit tests |
| **B1 backend read models** | `Core/Ports/ISiteReader.cs`, `IPageReader.cs`, `Core/Domain/Site*.cs`, `Adapters/Postgres/PostgresSiteReader.cs`, `PostgresPageReader.cs`, `Controllers/HostsController.cs` (list/detail/pages), `Controllers/PagesController.cs`, migration `015` (indexes) | Sites list, site detail, host pages, page detail, raw content; integration tests |

Lead merges F0 first (others rebase on it), then B0 and B1.

### Phase 2 — features (7 agents in parallel)
| Agent | Owns | Depends on |
|---|---|---|
| **F1 sites** | `features/sites/*` — sites list, site detail (Pages / Sitemap / Robots.txt tabs), page detail | F0, mocks |
| **F2 search** | `features/search/*` — search box, host facet, highlighted results | F0, mocks |
| **F3 crawl** | `features/crawl/*` — request-crawl dialog, live crawl screen, `useCrawlStream` hook (SignalR + polling fallback) | F0, mocks |
| **F4 analytics** | `features/analytics/*` — stat tiles, duration by host, pages by host, status codes, content types, time series | F0, mocks |
| **B2 search API** | `MeilisearchIndex.cs`, `SearchHit`, `PageToIndex`, `ISearchQuery`, `Controllers/SearchController.cs`, `SamoBot.Mcp/Tools` adjustment | B1 |
| **B3 live telemetry + sitemaps** | `Core/Crawling/*`, `CrawlEventTypes`, progress query, `Core/Sitemaps/*`, `Adapters/Http/HttpSitemapSource.cs`, `PostgresSitemapStore.cs`, migration `016`, worker loop registration, sitemaps endpoint | B1 |
| **B4 analytics API** | `Core/Ports/IAnalyticsReader.cs`, `Adapters/Postgres/PostgresAnalyticsReader.cs`, `Controllers/AnalyticsController.cs` | B1 |

Conflict rules: migrations are pre-numbered (015 → B1, 016 → B3). `Program.cs`, DI extension files, `routes.tsx`, `types/api.ts` and `mocks/` are lead-owned; a worker hands the lead a snippet. Every worker prompt carries: the contract, the design canvas link and the screen names it builds, its owned paths, the analyzer limits, and its done-check.

Done-check per worker
- Frontend: `npm run build && npm run lint` clean; screen matches the canvas in light and dark and at phone width; loading, empty and error states exist.
- Backend: `dotnet build SamoBot.sln -warnaserror`; unit tests; integration tests for every new SQL query.

### Phase 3 — integration (lead + 2 review agents)
1. Lead merges backend branches, then frontend branches, switches the frontend from mocks to the real API.
2. `docker compose up --build`, crawl one small site end to end, drive the dashboard with Playwright: sign-in (auth stubbed in test mode), request crawl, live counters move, sitemap and robots show after completion, search finds a crawled page, analytics numbers equal SQL counts.
3. Review agents in parallel: **code review** on the full diff; **security review** focused on token validation, cookie flags, CSRF on state-changing calls, the allowlist, raw-HTML display (text only), and SSRF from user-supplied seed URLs.
4. Lead fixes findings, updates `README.md`, deletes the stale `docs/crawling-dashboard-plan.md`, opens one PR if asked.

### Order and size
```
Phase 0  lead ───────┐
Phase 1  F0 ─ B0 ─ B1 (parallel) ──┐
Phase 2  F1 F2 F3 F4 B2 B3 B4 (parallel) ──┐
Phase 3  merge → e2e → review ─────────────┘
```
12 worker runs plus 2 reviewers. Frontend work never waits for backend because of the Phase 0 mocks.

## 5. What the lead needs from you before Phase 1
- A Google OAuth client ID (Web application) with the dashboard origin(s) as authorised JavaScript origins.
- Who may sign in: a list of emails, a Workspace domain, or any Google account.
- Approval of the design canvas, or the changes you want.
