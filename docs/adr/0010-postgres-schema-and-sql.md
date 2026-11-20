# 0010. PostgreSQL schema, SQL and migrations

Status: Accepted

## Decision

**SQL hygiene.**
- SQL never hard-codes a table name. Names come from `TableNames`.
- Values come in as parameters. No SQL text is built from input. ORDER BY clauses are fixed text chosen from an enum
  (`SiteSql`), never from input.
- `LikePattern` escapes user text so that it matches literally. Only the outer wildcards remain.
- Dapper keeps type handlers in global state, so `DapperTypeHandlers` registers them once. They map JSON text columns
  (string lists, kept response headers, page policies).

**Transactions and queues.**
- A parse result commits in one transaction: the parsed page with its policy, the new URLs, the stored links of the page and
  the parsed mark (`IParseBacklog.Complete`, `PostgresParseBacklog`). A parse claim uses a lease.
- The index backlog needs no lease, because indexing is idempotent (`PostgresIndexBacklog`).
- `NewUrlInserter` is the only place that adds URLs to the frontier. It applies the job scope (allowed hosts, maximum URLs) and
  adds the host rows that the claim query needs.
- `PageLinkWriter` replaces the links of a page inside the caller's transaction. It upserts the new links in one statement (a
  stored link keeps its `FirstSeenAt`), then deletes the links that are gone.
- `PostgresCrawlEventPublisher` stores an event and sends it with `pg_notify` as the flat event JSON. PostgreSQL rejects a
  notification of 8000 bytes or more, so a larger event is stored but not sent (`MaxNotifyBytes` = 7900). The poll endpoint still
  returns it.

**Links table (migration 025, `PageSql`).**
- `ToUrl` is the normalized absolute target, the same text as `DiscoveredUrls.NormalizedUrl`, at most 2048 characters. A btree
  on the raw text can exceed the index row limit, so the unique key and the incoming lookup use `md5("ToUrl")`.
- Incoming links are found by md5 equality, which uses the index. Text equality is checked as well, which rules out an md5
  collision. Only pages of the same job count, because the same normalized URL can exist in several jobs.
- At most `PageLinkPreparer.MaxLinksPerPage` (2000) distinct targets are stored for one page. A target longer than 2048 is
  skipped, the same limit as the URL columns. A canonical URL longer than its column is dropped, not cut, because a cut URL
  would point somewhere else.

**Sitemap keys (migration 016, `SitemapLimits.MaxLocBytes`).** `Loc` and `NormalizedLoc` are limited to 2048 UTF-8 bytes. A host
is at most 253 ASCII characters plus a port, so the largest key of `UQ_SitemapUrls_Host_Loc` and
`IX_SitemapUrls_Host_NormalizedLoc` is about 2.3 KB, under the 2704-byte btree limit. `NormalizedLoc` is the key to
`DiscoveredUrls.NormalizedUrl`.

**Large tables.**
- An index on a large table is built `CONCURRENTLY` (migrations 015 and 022), so crawling keeps writing. That needs
  `TransactionBehavior.None` and exactly one statement per `Execute.Sql`. A failed concurrent build leaves an INVALID index
  that `IF NOT EXISTS` then skips. Fix it with `DROP INDEX CONCURRENTLY <name>` and run the migrations again.
- The `DiscoveredUrls (Host, NormalizedUrl)` index was moved from 016 to 022 for that reason. A database that ran the old
  016 already has it, and `IF NOT EXISTS` skips it.
- New columns are added with a constant default or NULL, so PostgreSQL does not rewrite the table (migration 024).
- Migration 018 is intentionally empty. Migration 021 replaced its full-table UPDATE with a narrower one that re-sends one
  document per URL.
- **Readers handle one shape (migration 029).** Every parsed document has a policy (`ParsedDocuments.Policy` is NOT NULL). Documents
  parsed before migration 026 got the default policy that readers used to assume, with their old robots meta text kept as the
  one entry of `directives`. The column `RobotsDirectives` is gone. Old `FetchCompleted` events got `pageId` and `host` from
  `DiscoveredUrls`, and the few whose URL no longer exists were deleted, so every such event links to its page. A page that was
  fetched but not parsed yet still has no policy, and its stored copy may be shown (`PagePolicy.Default`). That is a state, not
  legacy data. The links of pages parsed before migration 025 come only from parsing again, through the `reprocess` action of
  their job. The migration does not reopen finished jobs, because that would start crawling after a deploy.
- Migration 026 stores `UrlFetches.ResponseHeaders` (the headers the directive sources read) and `ParsedDocuments.Policy`.

**Read models.**
- The analytics SQL reads `UrlFetches` in a `FetchedAt` window (`IX_UrlFetches_FetchedAt`) and joins `DiscoveredUrls` by primary
  key. Timeseries buckets are UTC. The series runs from the first bucket of the window to the bucket of `@To`, with zero where
  nothing was fetched. The first bucket starts at the truncated hour or day but counts only fetches from `@From`. For the "all"
  range the series starts at the oldest fetch, cut to `@Floor` (at most `AnalyticsNames.MaxBuckets` buckets).
- A page is a `DiscoveredUrls` row, and its latest fetch is the row at `LastFetchId` (`PageSql`).
- The sites overview scans every URL, so `SiteOverviewCache` keeps it briefly, and one load at a time serves all callers.
