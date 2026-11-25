# Architecture Decision Records

The code carries no comments (see [ADR 0001](0001-decisions-live-in-adrs-not-comments.md)). A non-obvious decision is recorded here.
Each record names the code it is about, so `grep` for a symbol finds its reasons.

| ADR | Decision |
|---|---|
| [0001](0001-decisions-live-in-adrs-not-comments.md) | Decisions live in ADRs, not in code comments |
| [0002](0002-network-safety.md) | Network safety (SSRF) |
| [0003](0003-timeouts-and-deadlines.md) | Timeouts and deadlines |
| [0004](0004-frontier-pacing-and-politeness.md) | Frontier, in-flight hold and politeness |
| [0005](0005-crawl-directives.md) | Crawl directives |
| [0006](0006-stored-content-and-markdown.md) | Stored content and Markdown |
| [0007](0007-search-index.md) | Search index |
| [0008](0008-sitemaps.md) | Sitemaps |
| [0009](0009-api-security.md) | API security |
| [0010](0010-postgres-schema-and-sql.md) | PostgreSQL schema, SQL and migrations |
| [0011](0011-crawl-jobs-and-links.md) | Crawl jobs, links and recrawls |
| [0012](0012-api-contract.md) | API contract |
| [0013](0013-background-loops.md) | Background loops and shutdown |
| [0014](0014-testing.md) | Testing |
| [0015](0015-dashboard-frontend.md) | Dashboard frontend |
| [0016](0016-content-processing-pipeline.md) | Content processing pipeline (proposed) |

New record: copy the shape of an existing one (Status, Context, Decision, Consequences) and add a row above.
