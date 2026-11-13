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
DASHBOARD_API_KEY=<secret> docker compose up --build
```

This starts PostgreSQL, RustFS (S3 storage, console at http://localhost:9001/rustfs/console/), Meilisearch, the migrations, the worker and the API (port 5080).
Send the key in the header `X-Api-Key`. The dashboard is in `src/frontend/`.

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
