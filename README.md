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
   |  crawl loop --> RustFS (HTML)                        |
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
cp .env.example .env   # then set DASHBOARD_API_KEY in .env
docker compose up --build
```

This starts PostgreSQL, RustFS (S3 storage, console at http://localhost:9001/rustfs/console/), Meilisearch, the migrations, the worker and the API (port 5080).
Send the key in the header `X-Api-Key` (MCP server and scripts). The dashboard is in `src/frontend/`.

### Dashboard sign-in (Google)

The dashboard signs in with Google and the API issues a cookie session (`samobot.session`: HttpOnly, SameSite=Lax, 8 hours sliding, Secure outside Development).
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

Tests:

```bash
dotnet build SamoBot.sln -warnaserror
dotnet test src/SamoBot.Tests --filter "Category!=Integration"
dotnet test src/SamoBot.Tests --filter "Category=Integration"   # needs Docker
```

## The claim pattern

Every stage does the same three steps: claim rows with a lease, do the work, record one result.

- **Crawl:** a claim takes at most one due URL for each host and locks the host row. The host row also holds the
  delay until the next request, so politeness needs no extra service.
- **Parse:** a claim leases stored fetches with `FOR UPDATE SKIP LOCKED`. One transaction stores the page,
  adds the in-scope links and marks the fetch parsed.
- **Index:** pages with no `IndexedAt` go to Meilisearch in one batch. Indexing is idempotent.

A worker that dies leaves a lease. When the lease ends, the next claim takes the row again.
A job is complete when no URL is in flight, no URL waits for its first fetch or a retry, and no page waits for the parser.

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

