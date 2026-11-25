# 0016. Content processing pipeline

Status: Proposed (2026-11). Nothing of it is implemented.

## Context

Every stored page should go through further steps: (1) clean Markdown, (2) chunks and embeddings, (3) LLM analysis. The steps
are different kinds of work. Markdown is CPU work, embeddings are batchable API calls, and analysis is a slow and paid API call.
So each step needs its own concurrency, retries, status and re-run.

What the code does today, and what the design must respect:

- Every stage claims rows with a lease, does the work and records one result in one transaction (ADR 0004, ADR 0010).
  `PollingWorker<THandler>` runs each stage (ADR 0013). The sitemap queue is the most complete example: a queue table, a lease,
  an attempt counter with a maximum, and a completion that writes nothing when another worker took the lease (ADR 0008).
- `IMarkdownConverter` (`string Convert(string html, Uri pageUrl)`) already exists. The API calls it on demand and stores
  nothing (ADR 0006).
- The stored object of a URL is overwritten by a recrawl (ADR 0006), so a derived result can only describe the latest fetch.
- No content hash exists, so the system cannot tell that a recrawl returned the same body.
- The parse queue has no attempt limit (ADR 0013). That is acceptable for free CPU work, not for paid API calls.
- `IContentStore.Save` names objects after the URL and forces `.html`, so it cannot hold derived files.
- `ContentState` is derived from facts. A value per step would force a change of `ContentStates`, the SQL and the frontend for
  every new step.
- `postgres:16-alpine` does not contain `pgvector`.
- A page that needs JavaScript but was fetched over plain HTTP is stored as an empty shell.

## Decision

**One generic task queue in PostgreSQL.** A new table `ProcessingTasks` holds one row per page per step. No broker and no job
library is added. The migration takes the next free number (027 is used by `DropRespectRobots`).

| Column | Type | Meaning |
|---|---|---|
| `Id` | bigint identity, PK | |
| `DiscoveredUrlId` | int, FK to `DiscoveredUrls`, on delete cascade | The page |
| `Step` | varchar(64) | Step name, for example `markdown` |
| `Status` | varchar(20) | `Pending`, `Running`, `Submitted`, `Succeeded`, `Skipped`, `Failed` |
| `UrlFetchId` | int, nullable, FK to `UrlFetches`, on delete set null | The fetch this result describes |
| `StepVersion` | int | Version of the step code that produced the result |
| `InputHash` | char(64), nullable | SHA-256 of step name, version, configuration and input hashes |
| `OutputHash` | char(64), nullable | SHA-256 of the main output |
| `Attempts` | int, default 0 | Raised by each claim |
| `NextAttemptAt` | timestamptz | Earliest next claim (backoff, rate limit, batch poll) |
| `LeaseUntil` | timestamptz, nullable | Lease of the current claim |
| `ErrorCode`, `ErrorMessage` | varchar(64), text | Stable reason (`timeout`, `rate-limited`, `thin-content`, `refusal`) and detail |
| `Metrics` | jsonb, nullable | Duration, tokens, cost, chunk count, quality score |
| `CreatedAt`, `StartedAt`, `CompletedAt` | timestamptz | |

- `UNIQUE (DiscoveredUrlId, Step)`: one current state per page and step. Tasks are keyed by page, not by fetch, because the raw
  object of an older fetch no longer exists.
- A partial index `(Step, NextAttemptAt) WHERE Status IN ('Pending', 'Running')` serves the claim. `(Step, Status)` serves counts.
- `Skipped` is a successful end state with a reason. "Stale" is not a status: it is `Succeeded` with an older `StepVersion`.

**Claim.** The claim follows the sitemap queue. A `Running` task with an expired lease belongs to a crashed worker and is
claimed again.

```sql
WITH picked AS (
    SELECT t."Id" FROM "ProcessingTasks" t
    WHERE t."Step" = @Step AND t."Attempts" < @MaxAttempts
      AND ((t."Status" = 'Pending' AND t."NextAttemptAt" <= @Now)
        OR (t."Status" = 'Running' AND t."LeaseUntil" < @Now))
    ORDER BY t."NextAttemptAt", t."Id"
    LIMIT @BatchSize
    FOR UPDATE SKIP LOCKED)
UPDATE "ProcessingTasks" t
SET "Status" = 'Running', "LeaseUntil" = @Now + make_interval(secs => @LeaseSeconds),
    "Attempts" = t."Attempts" + 1, "StartedAt" = @Now
FROM picked p WHERE p."Id" = t."Id"
RETURNING t.*;
```

**Steps (`IProcessingStep`, `StepDescriptor`, `StepResult`).**
- A step is one class. It declares `StepDescriptor(Name, Version, DependsOn)` and a configuration fingerprint (model id, prompt
  hash, chunk size), and it returns a `StepResult`. It never throws for an expected failure.
- One composition method lists the steps (`AddProcessingStep<TStep>`), in the style of `AddCrawlDirectives`. It registers the
  step and a `StepWorkHandler<TStep>`, and the worker runs one `PollingWorker` per step.
- Results are types, in the style of `CrawlOutcome`:

| Result | New task state |
|---|---|
| `Succeeded(OutputHash)` | `Succeeded`, `Attempts` 0, dependent steps queued |
| `Skipped(Reason)` | `Skipped` with `ErrorCode` = reason; dependent steps are not queued |
| `RetryLater(Reason, After)` | `Pending`, `NextAttemptAt` = now + (`After` or the backoff); `Failed` once `Attempts` reaches the maximum |
| `Failed(Reason, Detail)` | `Failed` with `ErrorCode` and `ErrorMessage` |

- The backoff has the shape of `BackoffPolicy.DelayFor`, with a base and maximum per step and jitter. A provider `Retry-After`
  wins. A sweep marks a task that ran out of attempts through expired leases as `Failed` with `lease-expired`.

**Dependencies form a DAG.** Today: `markdown` -> `chunk` -> `embed`, and `markdown` -> `analyze`. A linear list would make
analysis wait for embeddings and let one block the other. The registry checks names, dependencies and cycles at startup. A
`Succeeded` completion queues, in the same transaction, a `Pending` task for each dependent step whose dependencies have all
succeeded. A step with two dependencies takes `pg_advisory_xact_lock` on the page id first.

**Change detection by hash, not by time.** A dependent task whose stored `InputHash` equals the new one and whose status is
`Succeeded` stays as it is. A recrawl that gives byte-identical Markdown therefore neither embeds again nor calls the LLM. A new
`Version` makes results stale. A free step re-runs stale tasks automatically. A paid step (the default, `RerunPolicy.Manual`)
waits until a person starts a re-run for a scope after seeing the count and the estimated tokens.

**Execution is at least once.** Every write is idempotent: artifacts are content-addressed or upserted, chunks are replaced in
one transaction (as `PageLinkWriter` does), and a completion writes nothing when the lease was taken over. The lease of a paid
step must be longer than its longest call.

**Start after parsing.** Parsing decides the policy and the canonical page. `ParsedDocuments.PipelinePlannedAt` works like
`IndexedAt`: a re-parse resets it, and a `PipelinePlanHandler` creates the root tasks. A page is processed when its policy allows
indexing and it is not a copy of a canonical page, the same rule as the search index (ADR 0007).

**Providers behind ports.** `Core` keeps no I/O packages. It defines narrow ports (`IArtifactStore`, `ITextEmbedder`,
`ILanguageModel`, the existing `IMarkdownConverter`), and `Adapters` implements them. Provider types such as
`Microsoft.Extensions.AI` stay inside the adapters.

**Per-step settings (`Processing:Steps:<name>`).** Each step has its own batch, concurrency, lease, maximum attempts and an
`Enabled` flag (the brake for paid steps). Proposed starting values:

| Step | Work | Batch | Lease | Max attempts |
|---|---|---|---|---|
| `markdown` | CPU | 20 | 2 min | 3 |
| `chunk` | CPU | 50 | 2 min | 3 |
| `embed` | batchable API, token bucket, honours `Retry-After` | 16 pages | 5 min | 6 |
| `analyze` | slow paid API, budget check before each batch | 4 | 10 min | 4 |

**Outputs.**
- `ProcessingArtifacts` holds one artifact per page and kind (`UNIQUE (DiscoveredUrlId, Kind)`), with a content hash, and either
  an object name (large text such as Markdown, content-addressed in object storage) or inline JSON (analysis).
- `PageChunks` holds chunks with their heading path, hash and a `vector` column. Vectors live in PostgreSQL with `pgvector` and an
  HNSW index, not in a separate vector database.
- Chunking is heading-aware and size-bounded, and never splits a fenced code block or a table that fits.

**Status.** The API gets per-page step status, artifact content (with the `noarchive` rule of `/content`), re-run endpoints with
a dry run, and a summary. `ContentState` does not change. Phase 1 publishes no crawl events per step; the dashboard polls.

**LLM analysis.** One request per page, a fixed system prompt first and the page text last, output constrained to a JSON schema.
Page text is untrusted: the request has no tools, and nothing acts on the output automatically. Cost controls in order of effect:
do not call (eligibility, length limits, hash skip), no automatic re-runs, a daily budget per step, the Message Batches API,
prompt caching, a tight output limit.

## Options considered

| Option | Why not |
|---|---|
| A timestamp column per stage (`MarkdownAt`, `EmbeddedAt`) with its own backlog, as for parse and index | Every step needs a migration, a port, SQL and a `ContentStates` change. No attempts, reasons or versions. |
| Markdown inside `ParsePageHandler` | A converter bug would block link discovery. The step could not be re-run or versioned alone. |
| A job library on PostgreSQL (Hangfire, Quartz.NET, Wolverine, TickerQ) | A second schema and serialisation next to a tested lease idiom. Status, hashing and artifacts would still have to be built. |
| A message broker (RabbitMQ, Redis Streams, Kafka) | New infrastructure. The project removed its broker on purpose. |
| A workflow engine (Temporal) | A new server and programming model for four steps. |
| A Python sidecar for all processing | A second runtime and deployment. The ports keep this possible for a single step. |
| A linear pipeline | Couples the independent embedding and analysis branches. |
| Tasks keyed by fetch | Older raw objects no longer exist, and the table would grow with every recrawl. |
| A dedicated vector database (Qdrant, Weaviate, Milvus) | New infrastructure against the one-database direction. |
| Hosted page converters or LLM-based conversion | Content leaves the system, cost per page, and invented text. |

## Consequences

- A new step is one class and one registration line. No existing step, handler, SQL or migration changes.
- Phase 1 delivers the framework and the Markdown step with a dashboard view and a re-run. Phase 2 adds `pgvector`, chunks,
  embeddings and semantic search (and MCP tools for Markdown and semantic search). Phase 3 adds LLM analysis with a budget and
  batches. Phase 4 adds worker roles, `UrlFetches.ContentHash`, hybrid search and artifact clean-up.
- The `pgvector/pgvector:pg16` image is Debian, not Alpine. Text collation differs, so an existing data volume needs a `REINDEX`
  of text indexes or a dump and restore.
- `StepWorkHandler` would catch exceptions per item, so one bad page does not fail its batch. That breaks the rule that only
  `PollingWorker` catches `Exception` (ADR 0013) and needs the owner's decision. The alternative is to let the lease expire.
- `ProcessingTasks` grows with pages x steps, and `PageChunks` with 5 to 20 vectors of 4 KB per page.
- Open questions for the owner: which pages to process, Markdown in object storage or a `text` column, the per-item catch,
  duplicates across jobs, whether page content may go to external providers, languages, corpus size, the analysis fields and
  budget, the consumers of the results, automatic re-fetch of empty JavaScript shells, history, and non-HTML documents.
- The interfaces and SQL above are sketches. Nothing was compiled or run.
