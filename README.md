# SamoBot

SamoBot is a polite web crawler with a live dashboard. It obeys robots.txt and the directives of each page, paces every host
by how fast it answers, and refuses to connect to private addresses. PostgreSQL is the only queue: the frontier, the fetches,
the parsed pages, the jobs and the events all live in one database. Crawled pages are indexed in Meilisearch, and an MCP server
makes that search available to AI assistants.

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

## How to run

```bash
cp .env.example .env
openssl rand -hex 32   # put the output in .env as DASHBOARD_API_KEY
docker compose up --build
```

The dashboard is on http://localhost:8081 and the API on port 5080. Google sign-in, the API key, rate limits, JavaScript
rendering and the tests are described in [docs/operations.md](docs/operations.md). To search the crawled pages from an AI
assistant, see the [MCP server](src/SamoBot.Mcp/README.md).

## Interesting parts

### The claim pattern

Every stage of the worker does the same three steps: claim rows with a lease, do the work, record one result in one
transaction. A crawl claim takes at most one due URL for each host and locks the host row, and that row also holds the time of
the next allowed request. A worker that dies leaves only a lease, and when the lease ends the next claim takes the row again.
This is why PostgreSQL can be the only queue. See [the claim pattern](docs/architecture.md#the-claim-pattern),
[ADR 0004](docs/adr/0004-frontier-pacing-and-politeness.md) and [ADR 0010](docs/adr/0010-postgres-schema-and-sql.md).

### SSRF and DNS rebinding defence

Checking the URL text is not enough, because a public name can resolve to a private address and a redirect can point at one.
So the check runs at connect time: `SafeConnect` resolves the host, drops every address that `IpAddressPolicy` denies (private,
loopback, link-local and metadata, CGNAT, and IPv6 forms that carry such an IPv4 address) and connects to an allowed one by IP.
The request URL is not rewritten, so the Host header and the TLS server name stay the host name, and cached addresses are
checked again on every use. Headless Chrome resolves names itself, so its container sits on a network that cannot reach private
ranges. See [network safety](docs/architecture.md#network-safety-ssrf) and [ADR 0002](docs/adr/0002-network-safety.md).

### Adaptive politeness

The robots.txt Crawl-delay, or 1 s, is the minimum delay between two requests to a host. After each fetch `PolitenessPolicy`
sets the next delay: the larger of the base delay and 5 times the moving average of the response time, doubled for each
failure in a row, capped at `Crawler:MaxAdaptiveDelaySeconds`, and never shorter than the `Retry-After` of a 429 or 503. A claim holds the host for the longest possible fetch, so a slow server never gets a second
concurrent request. Completing the URL releases the hold in the same transaction. See
[politeness](docs/architecture.md#politeness-adaptive-delay) and [ADR 0004](docs/adr/0004-frontier-pacing-and-politeness.md).

### Crawl directives

robots.txt is always obeyed, and no job or setting turns it off. Meta robots tags, the X-Robots-Tag header, canonical links,
meta refresh, `<base href>` and `rel="nofollow"`, `"sponsored"` and `"ugc"` are all decided in one place,
`src/SamoBot.Core/Directives`, which produces a `PagePolicy` for each page. The strictest value wins when sources disagree.
Support for a new directive is one class registered in `AddCrawlDirectives`. See
[crawl directives](docs/architecture.md#crawl-directives-what-sites-allow) and [ADR 0005](docs/adr/0005-crawl-directives.md).

## Projects

`Core` (domain, use cases, port interfaces, no I/O packages), `Adapters` (PostgreSQL, HTTP, browser, S3 storage, Meilisearch,
HTML), `Worker`, `Api`, `Migrations`, `SamoBot.Mcp` (search server for AI tools). The dashboard is in `src/frontend/`.

The compiler enforces the dependency rule: `Core` references no project.

## Quality gates

The build fails when a method or function breaks the analyzer limits for C# and TypeScript: complexity, length, parameters,
nesting and file size (see [the limits](docs/architecture.md#limits-the-build-enforces-them)). Source files have no comments.
Decisions live in ADRs ([ADR 0001](docs/adr/0001-decisions-live-in-adrs-not-comments.md)), and a baseline that may only shrink
keeps it that way. Stryker.NET mutation testing checks the tests of `SamoBot.Core` and fails below a score of 75 %
([mutation testing](docs/mutation-testing.md)).

## Documentation

- [docs/operations.md](docs/operations.md): running, configuration, security settings and tests
- [docs/architecture.md](docs/architecture.md): how the crawler works and the limits the build enforces
- [docs/api-contract.md](docs/api-contract.md): the HTTP API of the dashboard
- [docs/adr/](docs/adr/README.md): the architecture decision records
- [docs/mutation-testing.md](docs/mutation-testing.md): mutation testing with Stryker.NET
- [src/SamoBot.Mcp/README.md](src/SamoBot.Mcp/README.md): the MCP search server
