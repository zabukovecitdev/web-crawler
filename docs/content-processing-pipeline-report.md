# Content processing pipeline: investigation and recommendation

Status: **research only. Nothing here is implemented.** Date: 2026-11-15.

The goal is to process every fetched document in separate, extensible steps: (1) clean Markdown, (2) chunking and embeddings, (3) LLM analysis. These are different kinds of work (CPU-bound, batchable API calls, slow and expensive API calls), so each must run as its own job with its own concurrency, retries and status.

## 1. Summary

**Recommendation:** add one generic, PostgreSQL-backed task queue (`ProcessingTasks`, one row per page per step) and run each step with the existing `PollingWorker<THandler>` loop. A step is one class that implements `IProcessingStep`, declares its name, version and dependencies, and is registered in one composition line. No new infrastructure is needed for phases 1 and 3; phase 2 adds the `pgvector` extension to the existing PostgreSQL.

Key design points:

- **Reuse what exists.** The repo already claims rows with a lease (`FOR UPDATE SKIP LOCKED`), completes them in one transaction, and counts attempts. The new queue uses the same idiom, generalised by a `Step` column, so adding a step needs no migration and no new SQL.
- **Steps form a DAG, declared by each step** (`DependsOn`). Today it is a small tree: `markdown → chunk → embed` and `markdown → analyze`.
- **Change detection by hash, not by time.** Each task stores an `InputHash` (step version + configuration + hashes of its inputs) and an `OutputHash`. A recrawl whose cleaned Markdown is byte-identical does not re-embed and does not call the LLM. This is the main cost control.
- **Providers behind ports.** `Core` defines `IMarkdownConverter`, `ITextEmbedder`, `ILanguageModel`; `Adapters` implements them. `Core` stays free of I/O packages, as the project rules require.
- **Phase 1** delivers the framework and the Markdown step end to end, including a dashboard view and a re-run button.

Things I could not verify are listed in section 12.

## 2. What exists today

### 2.1 Stack

| Area | Finding | Source |
|---|---|---|
| Language | C# on .NET 10, nullable enabled, warnings are errors | `Directory.Build.props:4-9` |
| Projects | `Core` (domain, use cases, ports; no I/O packages), `Adapters`, `Worker`, `Api`, `Migrations`, `Mcp`, `Tests` | `README.md:25-27`, `docs/refactoring-plan.md` §1.2 |
| Database | PostgreSQL 16 (`postgres:16-alpine`), Npgsql 10.0.1 + Dapper 2.1.66, FluentMigrator 7.2.0 | `compose.yaml:9`, `Directory.Packages.props` |
| Object storage | RustFS (S3 API) through the Minio client 7.0.0, bucket `samobot-content` | `compose.yaml:26`, `src/SamoBot.Worker/appsettings.json:5-13` |
| Search | Meilisearch v1.11, index `pages` | `compose.yaml:44`, `src/SamoBot.Worker/appsettings.json:14-18` |
| JS rendering | Optional headless Chrome over CDP with Playwright 1.49.0 (compose profile `js`) | `compose.yaml:75-76`, `src/SamoBot.Adapters/Browser/BrowserFetcher.cs` |
| HTML parsing | HtmlAgilityPack 1.11.71 | `src/SamoBot.Adapters/Html/HtmlAgilityPackParser.cs:9-35` |
| Queue | **"PostgreSQL is the only queue."** RabbitMQ and Redis were removed on purpose | `README.md:3`, `docs/refactoring-plan.md` §1.1 |
| Frontend | React + Vite + TanStack Query + shadcn/ui in `src/frontend` | `docs/dashboard-v2-plan.md` |
| Build gates | Method ≤ 30 lines, cyclomatic complexity ≤ 8, ≤ 6 parameters, nesting ≤ 2, file ≤ 300 lines; Stryker mutation testing on `SamoBot.Core` with a break threshold of 75 | `SonarLint.xml:5-9`, `.editorconfig:5-11`, `CodeMetricsConfig.txt`, `src/SamoBot.Tests/stryker-config.json` |

### 2.2 Data model of a crawl

- `CrawlJobs`: one crawl request (seeds, depth, `UseJsRendering`, `AllowedHosts`). `013_CreateCrawlJobsAndEvents.cs`, `014_RedesignFrontier.cs`.
- `DiscoveredUrls`: the frontier. One row per URL **per job** (unique on `CrawlJobId, NormalizedUrl`), with `LeaseUntil`, `FailCount`, `FailReason`, `LastFetchId`. The dashboard calls this row a "page" (`src/SamoBot.Adapters/Postgres/PageSql.cs:3`).
- `UrlFetches`: the fetch history of a URL (`StatusCode`, `ContentType`, `ObjectName`, `ParsedAt`, `LeaseUntil`, `ResponseHeaders`). `004_CreateUrlFetchesTable.cs`, `007`, `014`, `026`.
- `ParsedDocuments`: one row per fetch (unique `UrlFetchId`): title, description, `BodyText`, headings, `Policy` (index/follow/snippet/archive), `IndexedAt`. `009_CreateParsedDocumentsTable.cs`, `014`, `026`.
- `CrawlJobEvents`: the event log that feeds the live dashboard. `013_CreateCrawlJobsAndEvents.cs`.

Only successful HTML is stored (`src/SamoBot.Core/Crawling/FetchRecorder.cs:36-44`, `src/SamoBot.Core/Domain/StoredMediaTypes.cs:7-10`). So the pipeline input is always HTML; PDFs and other types never reach it today.

### 2.3 The existing job pattern

Every stage follows "claim rows with a lease, do the work, record one result":

- `IWorkHandler.HandleBatch` returns the number of processed items (`src/SamoBot.Core/Common/IWorkHandler.cs:3-7`).
- `PollingWorker<THandler>` runs one handler in a loop with a DI scope per batch; it waits 1 s when idle and 5 s after an exception (`src/SamoBot.Worker/PollingWorker.cs:13-48`, `PollingDelays.cs`).
- The worker registers five loops: crawl, parse, index, job completion, sitemaps (`src/SamoBot.Worker/Program.cs:18-22`).
- The **parse queue** is not a table. It is the predicate `ParsedAt IS NULL AND ObjectName IS NOT NULL` on `UrlFetches`, claimed with `FOR UPDATE ... SKIP LOCKED` and a 5-minute lease (`src/SamoBot.Adapters/Postgres/PostgresParseBacklog.cs:13-36`, `src/SamoBot.Core/Parsing/ParseBatchHandler.cs:8-16`). Completion writes the page, the new URLs, the links and the parsed mark in one transaction (`PostgresParseBacklog.cs:71-82`).
- The **index queue** is the predicate `IndexedAt IS NULL` on `ParsedDocuments` with no lease, because indexing is idempotent (`PostgresIndexBacklog.cs:8-27`). A re-parse resets `IndexedAt` to `NULL` (`PostgresParseBacklog.cs:51`), and migrations re-send documents by setting it to `NULL` (`021_ReindexLatestDocuments.cs`).
- The **sitemap queue** is the most complete example: a queue table, a lease, an `Attempts` counter with a maximum, and a completion that writes nothing when another worker took over the lease (`src/SamoBot.Core/Sitemaps/SitemapBatchHandler.cs:13-21`, `PostgresSitemapStore.cs:37-44` and `:114-116`, `023_AddAttemptsToSitemapRequests.cs`).

### 2.4 How status reaches the dashboard

- The page state is **derived**, not stored: `ContentStates.Derive` maps facts (status code, object name, parsed, indexed) to the `ContentState` enum (`src/SamoBot.Core/Domain/ContentStates.cs:20-50`, `PageSummary.cs:5-15`). The read model joins `DiscoveredUrls → UrlFetches (LastFetchId) → ParsedDocuments` (`PageSql.cs:6-16`).
- `GET /api/v1/pages/{id}` returns the detail; `GET /api/v1/pages/{id}/content` streams the stored HTML as `text/plain` with `nosniff` and a sandbox CSP, and returns 403 when the page policy is `noarchive` (`src/SamoBot.Api/Controllers/PagesController.cs:49-80`).
- Live updates: `ICrawlEventPublisher` inserts into `CrawlJobEvents` and calls `pg_notify('crawl_events', ...)`; the API listens and forwards to the SignalR group of the job (`PostgresCrawlEventPublisher.cs:17-43`, `src/SamoBot.Api/Hosting/CrawlEventListener.cs:84-93`). Event names are constants in `CrawlEventTypes.cs`.
- Frontend page view: `src/frontend/src/features/sites/PageContentCard.tsx`, `PageMetaCards.tsx`, `PagesTable.tsx`.

### 2.5 Existing extension idioms worth copying

- **Directives**: small interfaces (`IPagePolicyRule`, `ICanonicalSource`, ...), many implementations, all composed in one file. The comment says it directly: "Add a rule to support a new directive or signal; the other rules and the evaluator do not change" (`src/SamoBot.Core/Directives/Abstractions.cs:14-21`, `DirectivesServiceCollectionExtensions.cs:21-62`).
- **Fetchers**: `IEnumerable<IFetcher>` plus `Supports(url)` (`FetcherResolver.cs:6-12`).
- **Outcomes as types**: `CrawlOutcome` subclasses decide the next URL state (`docs/refactoring-plan.md` §1.7).

### 2.6 Findings that affect the pipeline design

1. **The stored HTML object is overwritten on a recrawl.** The object name is derived from the URL and its hash, not from the fetch (`src/SamoBot.Adapters/Storage/ObjectNameGenerator.cs:36-46`, `:56-57`). Every `UrlFetches` row of a URL points to the same object, which holds the newest body. Consequence: derived artifacts can only describe the *latest* fetch, so the pipeline is keyed by page (`DiscoveredUrlId`), not by fetch.
2. **There is no content hash anywhere.** `UrlFetches` has none, so the system cannot tell that a recrawl returned the same content.
3. **The parse queue has no attempt limit.** One fetch that always throws (for example a missing object) fails its whole batch (`Task.WhenAll` in `ParseBatchHandler.cs:14`), and is claimed again every 5 minutes forever. This is tolerable for free CPU work. It is not acceptable for paid API calls, so the new queue needs attempts, backoff and a terminal failure state.
4. **`IContentStore` cannot store derived files.** `Save` builds the name from the URL and forces a `.html` extension (`src/SamoBot.Adapters/Storage/MinioContentStore.cs:17-32`, `:73-76`). Derived artifacts need their own port.
5. **`ContentState` must not grow per step.** Adding `MarkdownReady`, `Embedded`, ... to the enum would force an edit of `ContentStates`, the SQL and the frontend for every new step. Step status should be a separate, data-driven collection.
6. **PostgreSQL image.** `postgres:16-alpine` does not contain `pgvector`. Tests use `postgres:16` (`src/SamoBot.Tests/Postgres/PostgresFixture.cs:12`).
7. **JS rendering already produces final DOM.** `BrowserFetcher` stores `page.ContentAsync()` after network idle (`BrowserFetcher.cs:98-113`), but only for jobs with `UseJsRendering` and with Chrome enabled (`:24`). Pages fetched over plain HTTP that need JavaScript are stored as empty shells.

## 3. Requirements derived from the brief

| Requirement | Design answer (section) |
|---|---|
| One responsibility per step; adding a step edits no existing step | `IProcessingStep`, registry, one registration line (5.1, 5.2) |
| Providers can be swapped | Ports in `Core`, adapters in `Adapters` (5.6) |
| Dependencies between steps | `DependsOn`, validated DAG (5.2) |
| Per-document, per-step status and stored outputs | `ProcessingTasks`, `ProcessingArtifacts` (4) |
| Idempotency and versioning | `StepVersion`, `InputHash`, `OutputHash`, lease check on completion (5.4) |
| Retries, backoff, failure reasons | `StepResult` types, `Attempts`, `NextAttemptAt`, `ErrorCode` (5.3) |
| Re-run one step for one document | `Requeue` + API endpoint (5.5) |
| Separate queues, concurrency, rate limits | One polling loop per step, per-step options (5.7) |
| Batching for embeddings | Batch port, chunk-hash reuse (7.3) |
| Cost control for LLM steps | Hash-based skipping, manual re-runs, budget, Batch API, caching (7.4) |

## 4. Proposed data model

All tables are added by one FluentMigrator migration (`027_CreateProcessingPipeline.cs`, id `20240101000027`). Table names must be added in both `src/SamoBot.Migrations/Constants/TableNames.cs` and `src/SamoBot.Adapters/Postgres/TableNames.cs`.

### 4.1 `ProcessingTasks` (the queue and the status record)

| Column | Type | Meaning |
|---|---|---|
| `Id` | bigint identity, PK | |
| `DiscoveredUrlId` | int, FK → `DiscoveredUrls` ON DELETE CASCADE | The page |
| `Step` | varchar(64) | Step name, for example `markdown` |
| `Status` | varchar(20) | `Pending`, `Running`, `Submitted`, `Succeeded`, `Skipped`, `Failed` |
| `UrlFetchId` | int, nullable, FK → `UrlFetches` ON DELETE SET NULL | The fetch this result describes |
| `StepVersion` | int | Version of the step code that produced the result |
| `InputHash` | char(64), nullable | SHA-256 of version + configuration + input hashes |
| `OutputHash` | char(64), nullable | SHA-256 of the main output |
| `Attempts` | int, default 0 | Incremented by each claim |
| `NextAttemptAt` | timestamptz | Earliest next claim (backoff, rate limit, batch poll) |
| `LeaseUntil` | timestamptz, nullable | Lease of the current claim |
| `ErrorCode` | varchar(64), nullable | Stable reason: `timeout`, `rate-limited`, `thin-content`, `refusal`, ... |
| `ErrorMessage` | text, nullable | Detail for a person, truncated |
| `Metrics` | jsonb, nullable | Duration, tokens in/out, cost, chunk count, quality score |
| `BatchId` | bigint, nullable, FK → `ProcessingBatches` | Only for provider batch jobs (phase 3) |
| `CreatedAt`, `StartedAt`, `CompletedAt` | timestamptz | |

Constraints and indexes:

- `UNIQUE (DiscoveredUrlId, Step)`: one current state per page per step.
- Partial index for the claim: `(Step, NextAttemptAt) WHERE Status IN ('Pending', 'Running')`.
- Index `(Step, Status)` for dashboard counts.

`Skipped` is a successful end state with a reason (for example the page is too short or the policy forbids processing). "Stale" is not a status; it is derived: `Status = 'Succeeded' AND StepVersion < current version`.

### 4.2 `ProcessingArtifacts` (step outputs)

| Column | Type | Meaning |
|---|---|---|
| `Id` | bigint identity, PK | |
| `DiscoveredUrlId` | int, FK ON DELETE CASCADE | |
| `Kind` | varchar(64) | `markdown`, `analysis`, ... |
| `Step` | varchar(64) | Producer |
| `ContentHash` | char(64) | SHA-256 of the content |
| `ObjectName` | varchar(2048), nullable | Location in object storage, for large text |
| `Inline` | jsonb, nullable | Small structured output (LLM analysis) |
| `ByteSize` | bigint | |
| `MediaType` | varchar(100) | `text/markdown`, `application/json` |
| `CreatedAt` | timestamptz | |

`UNIQUE (DiscoveredUrlId, Kind)`. Exactly one of `ObjectName` and `Inline` is set.

**Where the Markdown lives.** Recommended: object storage, with a content-addressed name such as `derived/markdown/ab/abcdef….md`. Reasons: it matches how raw HTML is stored, keeps rows small, makes writes idempotent (same content, same name), and avoids the overwrite problem of finding 2.6.1. The alternative is a `text` column in PostgreSQL, which is simpler and transactional; `ParsedDocuments.BodyText` already does this. Both work at this scale; see the open questions.

### 4.3 `PageChunks` (phase 2)

| Column | Type | Meaning |
|---|---|---|
| `Id` | bigint identity, PK | |
| `DiscoveredUrlId` | int, FK ON DELETE CASCADE | |
| `Ordinal` | int | Position in the document |
| `HeadingPath` | text | For example `Install > Linux > Docker` |
| `Content` | text | Chunk text |
| `TokenCount` | int | |
| `ContentHash` | char(64) | Hash of heading path + content |
| `EmbeddingModel` | varchar(100), nullable | Model id that produced the vector |
| `Embedding` | `vector(1024)`, nullable | `NULL` until the embed step ran |

`UNIQUE (DiscoveredUrlId, Ordinal)`; HNSW index on `Embedding` with `vector_cosine_ops`. Chunks are a typed table, not a generic artifact, because they are queried by vector.

### 4.4 `ProcessingBatches` (phase 3, provider batch jobs)

`Id`, `Step`, `Provider`, `ExternalBatchId`, `Status` (`Submitted`, `Ended`, `Collected`, `Failed`), `RequestCount`, `SubmittedAt`, `NextPollAt`, `CompletedAt`.

### 4.5 One column on an existing table

`ParsedDocuments.PipelinePlannedAt timestamptz NULL`. It works like `IndexedAt`: a new parsed document has `NULL`, and the planner (5.8) picks it up. This is the only change to an existing table in phase 1.

## 5. Proposed architecture

```
 parse loop ──► ParsedDocuments (PipelinePlannedAt IS NULL)
                      │
               PipelinePlanHandler  ── inserts root tasks for eligible pages
                      ▼
               ProcessingTasks  (Step, Status, InputHash, Attempts, Lease)
                 │         │          │           │
     StepWorkHandler<Markdown> <Chunk> <Embed> <Analyze>     one PollingWorker each
                 │         │          │           │
           IMarkdownConverter   ITextEmbedder   ILanguageModel     ports in Core
                 │                    │           │
          ProcessingArtifacts     PageChunks   ProcessingArtifacts
          + object storage        (pgvector)   (inline JSON)
```

### 5.1 Step interface

```csharp
// SamoBot.Core/Processing/IProcessingStep.cs
public interface IProcessingStep
{
    StepDescriptor Descriptor { get; }

    /// <summary>Processes one page. Never throws for an expected failure; it returns a result.</summary>
    Task<StepResult> Execute(StepContext context, CancellationToken ct);
}

/// <summary>Name, version and dependencies of a step. Raise Version when the output of the same input changes.</summary>
public sealed record StepDescriptor(string Name, int Version, IReadOnlyList<string> DependsOn)
{
    /// <summary>Settings that change the output (model id, prompt hash, chunk size). Part of the input hash.</summary>
    public string ConfigurationFingerprint { get; init; } = string.Empty;

    public RerunPolicy OnVersionChange { get; init; } = RerunPolicy.Manual;
}

public enum RerunPolicy { Manual, Automatic }

public sealed record StepContext
{
    public required long TaskId { get; init; }
    public required int PageId { get; init; }
    public required int FetchId { get; init; }
    public required string Url { get; init; }
    public required string RawObjectName { get; init; }
    public required int Attempt { get; init; }
    /// <summary>The artifacts of the steps this step depends on, by kind.</summary>
    public IReadOnlyDictionary<string, ArtifactRef> Inputs { get; init; } = new Dictionary<string, ArtifactRef>();
}
```

Results are types, in the same style as `CrawlOutcome`:

```csharp
// SamoBot.Core/Processing/StepResult.cs
public abstract record StepResult
{
    public StepMetrics Metrics { get; init; } = StepMetrics.None;
}

public sealed record Succeeded(string OutputHash) : StepResult;
public sealed record Skipped(string Reason) : StepResult;                        // end state, not an error
public sealed record RetryLater(string Reason, TimeSpan? After = null) : StepResult;   // transient
public sealed record Failed(string Reason, string Detail) : StepResult;          // permanent
```

Why this satisfies SOLID:

- **Single responsibility.** A step converts inputs to outputs. Claiming, leases, retries, hashing and fan-out live in `StepWorkHandler` and the task store.
- **Open/closed.** A new step is a new class and one registration line. No existing step, handler, SQL or migration changes.
- **Liskov.** Every step honours the same contract: no exception for expected failures, idempotent writes.
- **Interface segregation.** Steps do not see the queue. They receive a `StepContext` and use only the ports they need.
- **Dependency inversion.** Steps depend on ports defined in `Core`.

### 5.2 Registry and dependencies (DAG)

```csharp
// SamoBot.Core/Processing/StepRegistry.cs
public sealed class StepRegistry
{
    public StepRegistry(IEnumerable<StepDescriptor> descriptors) { /* validates: unique names, known dependencies, no cycle */ }

    public IReadOnlyList<StepDescriptor> Roots { get; }
    public IReadOnlyList<StepDescriptor> DependentsOf(string step);
    public StepDescriptor Find(string step);
}

// Composition, in the style of DirectivesServiceCollectionExtensions: the one place that lists the steps.
services
    .AddProcessingStep<MarkdownStep>()
    .AddProcessingStep<ChunkStep>()
    .AddProcessingStep<EmbedStep>()
    .AddProcessingStep<AnalyzeStep>();
```

`AddProcessingStep<TStep>` registers the step, its descriptor and `StepWorkHandler<TStep>`. The worker adds one hosted service per step, like the five lines in `src/SamoBot.Worker/Program.cs:18-22`. `Core` cannot do this itself, because it must not reference `Microsoft.Extensions.Hosting`.

**DAG or linear?** Declare a DAG, because the brief already contains a fork (embedding and analysis both read the Markdown and are independent). A linear list would make the LLM step wait for embeddings for no reason, and a failure in one would block the other. The cost of the DAG is small: validation at startup and one rule for joins (below).

**How downstream tasks are created.** When a task completes as `Succeeded`, the task store, in the same transaction, upserts a `Pending` task for each dependent step whose dependencies have all succeeded. A step with two or more dependencies needs a guard against two parents finishing at the same moment: the completion takes `pg_advisory_xact_lock` on the page id first. The planned tree has no joins yet, but the rule should be in place from the start.

An alternative is to create all tasks up front as `Waiting` and let the claim query check the dependencies. It shows "waiting" in the dashboard without extra work, but the claim SQL then needs the dependency graph in the database. I recommend enqueue-on-completion; the dashboard can show "not started" for a registered step without a row.

### 5.3 Claim, retry, backoff, failure

```csharp
// SamoBot.Core/Ports/IProcessingTaskStore.cs
public interface IProcessingTaskStore
{
    Task<IReadOnlyList<ClaimedTask>> Claim(string step, ClaimOptions options, CancellationToken ct);

    /// <summary>Records the result and queues the dependent steps, in one transaction. False when the lease was lost.</summary>
    Task<bool> Complete(ClaimedTask task, StepResult result, IReadOnlyList<string> dependents, CancellationToken ct);

    Task Requeue(RequeueRequest request, CancellationToken ct);
}

public sealed record ClaimOptions(int BatchSize, TimeSpan Lease, int MaxAttempts);
```

The claim follows the sitemap queue (`PostgresSitemapStore.cs:37-44`):

```sql
WITH picked AS (
    SELECT t."Id" FROM "ProcessingTasks" t
    WHERE t."Step" = @Step AND t."Attempts" < @MaxAttempts
      AND ((t."Status" = 'Pending' AND t."NextAttemptAt" <= @Now)
        OR (t."Status" = 'Running' AND t."LeaseUntil" < @Now))      -- a crashed worker
    ORDER BY t."NextAttemptAt", t."Id"
    LIMIT @BatchSize
    FOR UPDATE SKIP LOCKED)
UPDATE "ProcessingTasks" t
SET "Status" = 'Running', "LeaseUntil" = @Now + make_interval(secs => @LeaseSeconds),
    "Attempts" = t."Attempts" + 1, "StartedAt" = @Now
FROM picked p WHERE p."Id" = t."Id"
RETURNING t.*;
```

Result handling in `Complete`:

| Result | New state |
|---|---|
| `Succeeded` | `Succeeded`, `OutputHash` set, `Attempts = 0`, dependents queued |
| `Skipped` | `Skipped` with `ErrorCode = reason`; dependents are not queued |
| `RetryLater` | `Pending`, `NextAttemptAt = now + (After ?? backoff(Attempts))`; `Failed` when `Attempts >= MaxAttempts` |
| `Failed` | `Failed` with `ErrorCode` and `ErrorMessage` |

- **Backoff.** Reuse the shape of `BackoffPolicy.DelayFor` (`src/SamoBot.Core/Crawling/BackoffPolicy.cs:10-15`) with per-step base and maximum, plus jitter. A provider `Retry-After` wins over the computed delay.
- **Crashed workers.** A `Running` task with an expired lease is claimed again. A task that reaches `MaxAttempts` this way is set to `Failed` with `ErrorCode = 'lease-expired'` by a small sweep statement that the handler runs with each batch.
- **Unexpected exceptions.** Adapters translate expected failures to results, as `IFetcher` does ("never throws for an HTTP error status", `src/SamoBot.Core/Ports/IFetcher.cs:5`). For the unexpected ones, `StepWorkHandler` should catch per item and record `RetryLater("exception")`, so one bad page does not fail its batch (finding 2.6.3). Note that this deviates from the rule that `PollingWorker` is "the only place in the worker that catches `Exception`" (`PollingWorker.cs:11`). It is a deliberate decision for the owner; the alternative is to let the lease expire, which is slower and hides the error text.

### 5.4 Idempotency and versioning

- **Execution is at-least-once.** A lease can expire while a step still runs. Therefore every step writes idempotently: artifacts are content-addressed or upserted on `(DiscoveredUrlId, Kind)`; chunks are replaced in one transaction (the same approach as `PageLinkWriter.Replace`); `Complete` writes nothing when the lease was taken over. The lease of a paid step must be longer than its longest call, so that double execution stays rare.
- **`InputHash`** = SHA-256 of `(step name, StepVersion, ConfigurationFingerprint, input hashes)`. For the root step the input is the fetch id (later a raw content hash, see below). For other steps the inputs are the `OutputHash` values of their dependencies.
- **Skip unchanged work.** When a dependent task is queued and its stored `InputHash` equals the new one and its status is `Succeeded`, it stays as it is. Example: a recrawl changes only an advertisement block. The Markdown step runs (cheap), produces the same `OutputHash`, and no embedding or LLM call follows.
- **Version change.** Raising `Version` (new prompt, new chunk size, new converter rules) makes old results stale. For free steps (`RerunPolicy.Automatic`) a reconciler sets stale tasks back to `Pending`. For paid steps (`RerunPolicy.Manual`, the default) nothing happens until a person starts a re-run for a scope and sees the estimate first. A prompt edit must never silently re-bill the whole corpus.
- **Optional improvement.** Add `UrlFetches.ContentHash` (SHA-256 of the body, computed in `FetchRecorder` where the bytes are in memory). Then even the Markdown step is skipped for identical recrawls, and the hash is useful for duplicate detection in general. This touches the crawl path, so it is not part of phase 1.

### 5.5 Re-running

```csharp
public sealed record RequeueRequest
{
    public required string Step { get; init; }
    public int? PageId { get; init; }          // one document
    public string? Host { get; init; }         // or a scope
    public int? CrawlJobId { get; init; }
    public bool OnlyFailed { get; init; }
    public bool OnlyStale { get; init; }
    public bool Force { get; init; }           // clears InputHash, so the skip rule does not apply
}
```

- One step for one document: `POST /api/v1/pages/{id}/processing/{step}/rerun`. It sets the task to `Pending`, `Attempts = 0`, `NextAttemptAt = now`. Downstream steps re-run only when the output hash changes; `Force` on the downstream step re-runs it anyway.
- A scope: `POST /api/v1/processing/steps/{step}/rerun` with a body of the fields above and `dryRun: true` to return the count (and for paid steps the estimated tokens) before anything is queued.
- A new step added later is backfilled the same way: the planner creates tasks only for newly parsed pages, and a scoped re-run of the new step covers existing pages.

### 5.6 Provider ports (dependency inversion)

`Core` references only `FluentResults`, caching, DI abstractions and the robots parser (`src/SamoBot.Core/SamoBot.Core.csproj:3-8`). I recommend keeping that and defining narrow ports:

```csharp
// SamoBot.Core/Ports
public interface IMarkdownConverter
{
    MarkdownDocument Convert(Stream html, Uri pageUrl);          // CPU only
}

public sealed record MarkdownDocument(string Markdown, string Title, MarkdownQuality Quality);

public interface IArtifactStore
{
    Task<ArtifactRef> SaveText(ArtifactKey key, string content, CancellationToken ct);
    Task<string> LoadText(ArtifactRef artifact, CancellationToken ct);
}

public interface ITextEmbedder
{
    EmbeddingModelInfo Model { get; }                             // id, dimensions, max input tokens, max batch size
    Task<IReadOnlyList<ReadOnlyMemory<float>>> Embed(IReadOnlyList<string> texts, CancellationToken ct);
}

public interface ILanguageModel
{
    Task<LlmResponse> Generate(LlmRequest request, CancellationToken ct);
}

public sealed record LlmRequest(string System, string Input, string JsonSchema, int MaxOutputTokens);
public sealed record LlmResponse(string Json, LlmUsage Usage, LlmStop Stop);
```

Inside `Adapters`, `Microsoft.Extensions.AI` (`IChatClient`, `IEmbeddingGenerator`) is a good implementation detail: OpenAI, Ollama and the Anthropic SDK all provide it, so switching provider is configuration. I do not recommend exposing those types in `Core`: the ports above are smaller, and exceptions, rate limits and usage accounting are provider-specific and belong in the adapter.

### 5.7 Separate queues, concurrency and rate limits

Each step has its own polling loop and its own options, bound from configuration (`Processing:Steps:<name>`):

| Step | Kind of work | Batch | Concurrency | Lease | Max attempts | Notes |
|---|---|---|---|---|---|---|
| `markdown` | CPU | 20 | number of cores | 2 min | 3 | No external calls |
| `chunk` | CPU | 50 | cores | 2 min | 3 | Pure function |
| `embed` | Batchable API | 16 pages | 2 | 5 min | 6 | Token-bucket limiter; honours `Retry-After` |
| `analyze` | Slow, paid API | 4 | 2 | 10 min | 4 | Budget check before each batch; batch mode in phase 3 |

- **Rate limits.** With one worker process, a `TokenBucketRateLimiter` from `System.Threading.RateLimiting` (part of .NET) inside the provider adapter is enough. A 429 becomes `RetryLater(After)`. With several worker processes the limit must be shared; the simplest shared form is a lower per-process limit, and the exact form is a small counter table in PostgreSQL.
- **Scaling job types separately.** Add `Worker:Roles` (for example `crawl,parse,index` and `process`) so that compose can run a second worker container for processing with its own resources. Not needed in phase 1.
- **Pausing.** A per-step `Enabled` flag stops claiming. This is the emergency brake for paid steps.

### 5.8 Plugging into the crawl flow

The pipeline starts **after parsing**, not after fetching, because parsing decides what the site allows (`Policy`) and which page is canonical. No existing handler changes:

1. Migration 027 adds `ParsedDocuments.PipelinePlannedAt`.
2. A new `PipelinePlanHandler : IWorkHandler` claims parsed documents with `PipelinePlannedAt IS NULL`, keeps the newest document per URL (as `IndexBatchHandler.LatestPerUrl` does, `IndexBatchHandler.cs:33-34`), applies the eligibility rule, upserts the root tasks, and sets `PipelinePlannedAt`.
3. One line is added to the upsert in `PostgresParseBacklog.cs:45-51` so that a re-parse resets `PipelinePlannedAt` to `NULL`, exactly as it resets `IndexedAt`.

**Eligibility rule** (a small policy class, configurable): process a page when its policy allows indexing and it is not a non-canonical copy. This reuses the decision the search index already makes (`IndexBatchHandler.cs:26-27`) and avoids paying for duplicates and for pages the site asked not to index. Optional limits: allowed hosts, a per-job opt-in, language.

### 5.9 Status in the API and dashboard

- `GET /api/v1/pages/{id}/processing` → `[{ step, status, stale, attempts, errorCode, errorMessage, startedAt, completedAt, metrics, artifacts: [{ kind, byteSize, contentHash }] }]`. One entry per registered step; a step without a task is `NotStarted`.
- `GET /api/v1/pages/{id}/artifacts/{kind}` → the artifact as `text/plain`, with the same headers and the same `noarchive` rule as `GetContent` (`PagesController.cs:61-80`). The dashboard must render Markdown as text or through a sanitising renderer; it is derived from untrusted pages.
- `GET /api/v1/processing/summary` → counts per step and status, and tokens and cost for today.
- The re-run endpoints of section 5.5.
- Frontend: a "Processing" card and a "Markdown" tab in the page view (`src/frontend/src/features/sites/`).
- **Live events.** Publishing one `CrawlJobEvents` row per step per page would multiply the event volume. Recommended for phase 1: no events; the page view and the summary poll with TanStack Query. Later, publish only failures and periodic per-step counters.
- `ContentState` stays unchanged (finding 2.6.5).

The MCP server (`src/SamoBot.Mcp`) is a natural consumer: a `get_page_markdown` tool in phase 1 and a semantic search tool in phase 2.

## 6. Core sketch of the generic handler

```csharp
// SamoBot.Core/Processing/StepWorkHandler.cs
public sealed class StepWorkHandler<TStep>(
    TStep step,
    IProcessingTaskStore tasks,
    StepRegistry registry,
    StepOptionsSource options) : IWorkHandler
    where TStep : IProcessingStep
{
    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var settings = options.For(step.Descriptor.Name);
        if (!settings.Enabled)
        {
            return 0;
        }

        var claimed = await tasks.Claim(step.Descriptor.Name, settings.Claim, ct);
        await Parallel.ForEachAsync(claimed, settings.Parallelism(ct), (task, token) => Run(task, token));
        return claimed.Count;
    }

    private async ValueTask Run(ClaimedTask task, CancellationToken ct)
    {
        var result = await StepGuard.Execute(step, task.Context, ct);     // per-item isolation, see 5.3
        var dependents = registry.DependentsOf(step.Descriptor.Name).Select(d => d.Name).ToList();
        await tasks.Complete(task, result, dependents, ct);
    }
}
```

The Markdown step, to show how small a step is:

```csharp
// SamoBot.Core/Processing/Steps/MarkdownStep.cs
public sealed class MarkdownStep(IContentStore raw, IMarkdownConverter converter, IArtifactStore artifacts) : IProcessingStep
{
    public const string Name = "markdown";
    private const int MinCharacters = 200;

    public StepDescriptor Descriptor { get; } = new(Name, Version: 1, DependsOn: []) { OnVersionChange = RerunPolicy.Automatic };

    public async Task<StepResult> Execute(StepContext context, CancellationToken ct)
    {
        await using var html = await raw.Load(context.RawObjectName, ct);
        var document = converter.Convert(html, new Uri(context.Url));
        if (document.Markdown.Length < MinCharacters)
        {
            return new Skipped("thin-content");
        }

        var saved = await artifacts.SaveText(ArtifactKey.For(context, Name), document.Markdown, ct);
        return new Succeeded(saved.ContentHash) { Metrics = StepMetrics.Of(document.Quality) };
    }
}
```

## 7. Tooling for each step

Package versions and dates were read from NuGet on 2026-11-15.

### 7.1 HTML to clean Markdown

The work has three parts: find the main content, convert it, clean the result.

**Main content extraction**

| Option | Notes |
|---|---|
| **Semantic container rules on HtmlAgilityPack** (already in the repo; latest 1.13.1) | Prefer `<main>`, `<article>`, `[role=main]` and the known containers of documentation generators (`.markdown-body`, `.theme-doc-markdown`, `.md-content`, `.rst-content`, `#content`). Remove `nav`, `header`, `footer`, `aside`, `form`, `script`, `style`, `[aria-hidden=true]`, cookie and consent blocks. Precise for documentation sites; keeps code and tables. |
| **SmartReader** 0.11.1 (2026-07) | .NET port of Mozilla Readability, built on AngleSharp. Maintained. Tuned for articles; on documentation and landing pages it can drop tables, code or short sections. Good as a fallback. |
| Mozilla Readability.js inside the existing headless Chrome | The reference implementation, but it ties a CPU step to an optional, network-isolated container and is slow. Rejected. |
| Hosted or external tools (Jina Reader, Firecrawl, a Python sidecar with trafilatura or Docling, MarkItDown) | Good quality, but a new runtime or a paid service, and page content leaves the system. Rejected for phase 1; the `IMarkdownConverter` port allows it later. |
| LLM-based conversion (for example ReaderLM) | Expensive per page and can invent text. Rejected. |

Recommendation: a chain of extractors where the first confident one wins, the same idiom as `ICanonicalSource` ("the first source that finds one wins", `Abstractions.cs:38-42`): semantic container → SmartReader → whole `<body>` with boilerplate tags removed. Each returns a score (text length, link density), and the score is stored in `Metrics`.

**Conversion**

| Option | Notes |
|---|---|
| **ReverseMarkdown** 6.3.0 (2026-10) | Actively maintained, uses HtmlAgilityPack (no second DOM library), GitHub-flavoured output, tables, fenced code, configurable handling of unknown tags (drop, bypass, pass through). **Recommended.** |
| Html2Markdown 8.1.2 | Less configurable; fewer options for tables and code. |
| Own converter on AngleSharp (1.10.3) or HtmlAgilityPack | Full control, but lists, nested tables and whitespace rules are a long tail of work. Only if ReverseMarkdown output is not good enough. |

**Clean-up after conversion** (small pure functions, testable with golden files): resolve relative links and image URLs against the page URL and `<base>`; take the code language from `class="language-x"`; make the title the single `#` heading; collapse blank lines; drop empty links and "skip to content" or "edit this page" items; keep simple tables as Markdown tables and pass complex ones (row or column spans) through as HTML.

**JavaScript-rendered pages.** Pages of jobs with `UseJsRendering` are already stored as rendered DOM, so the step needs nothing special. For pages fetched over plain HTTP that are empty application shells, the step returns `Skipped("thin-content")`. A later option is to mark such a URL for a re-fetch with rendering (set `DiscoveredUrls.UseJsRendering` and `NextCrawlAt`); this changes crawl behaviour and cost, so it is an owner decision.

**Placement.** `IMarkdownConverter` in `Core/Ports`; the implementation in `Adapters/Markdown` (it uses HtmlAgilityPack, like `HtmlAgilityPackParser`). Tests: HTML fixtures from real documentation generators (Docusaurus, MkDocs Material, Sphinx, GitHub README, a blog, an SPA shell) with expected Markdown.

### 7.2 Chunking Markdown

| Strategy | Notes |
|---|---|
| Fixed size with overlap | Simple, but cuts code blocks and tables and loses the section context. |
| **Heading-aware, size-bounded** | Split at headings; keep the heading path with each chunk; never split inside a fenced code block or a table when it fits; merge very small sections; split long sections at paragraph boundaries with a small overlap. **Recommended.** |
| Semantic splitting (embedding similarity between sentences) | More API calls for small gains on structured documentation. |
| Contextual chunks (an LLM writes a short context for each chunk) or contextual embedding models | Better retrieval, but a paid call per chunk or a provider-specific model. A later option. |

Suggested parameters: target 400–600 tokens, hard maximum about 1,000, overlap 0 between sections and about 10 % inside a split section. The embedded text is `HeadingPath + "\n\n" + Content`.

Implementation: a small line scanner in `Core` (ATX headings and code fences). It is safe because the pipeline produces the Markdown itself, and it is pure, so it fits the mutation tests. Alternatives: Markdig 1.6.0 gives a full syntax tree if the scanner is not enough. `Microsoft.Extensions.DataIngestion` has header and section chunkers but is preview-only (11.0.0-preview), so I would not depend on it. The Semantic Kernel text chunker (1.82.0) is a large dependency for one function.

Token counts: `Microsoft.ML.Tokenizers` 3.0.0 behind an `ITokenCounter` port for exact OpenAI counts; for other models a character-based estimate with a safety margin is enough.

Chunk identity: `ContentHash` per chunk. On a re-run, vectors of unchanged chunks are kept and only new hashes are embedded.

### 7.3 Embeddings and vector storage

**Models**

| Option | Notes |
|---|---|
| Hosted: OpenAI `text-embedding-3-small` / `-large` | Cheap, adjustable dimensions, official `OpenAI` package 2.15.0 and `Microsoft.Extensions.AI.OpenAI` 11.0.0. |
| Hosted: Voyage AI (voyage-4 family) | Strong retrieval quality, multilingual, default 1,024 dimensions, a batch API. No official stable .NET SDK found (the `VoyageAI` package on NuGet is a `-dev` build), so a small typed `HttpClient` adapter. Anthropic itself offers no embedding model. |
| Local: Ollama as a compose service (`bge-m3`, `nomic-embed-text`, `qwen3-embedding`, `embeddinggemma`) with OllamaSharp 5.5.2 | No cost per token, content stays local, one new container. Slow without a GPU. |
| Local: in-process ONNX Runtime 1.32.0 + `Microsoft.ML.Tokenizers` | No new container, but tokenisation and pooling are hand-written and the worker image grows. |

The choice depends on three owner answers: may content leave the server, is the content multilingual (Slovenian), and what is the corpus size. At documentation scale the cost of hosted embeddings is small: 100,000 pages × about 1,500 tokens is 150 million tokens, which is a single-digit to low double-digit dollar amount at current list prices. My default would be a hosted model at 1,024 dimensions, with Ollama as the alternative when content must stay local. The port makes this a configuration change, but **a model change always means re-embedding everything** (and a new migration when the dimensions change), so it is worth deciding once.

**Batching.** `ITextEmbedder.Embed` takes a list. The embed step sends all new chunks of the claimed pages in as few requests as the model's batch limit allows. Provider batch APIs (asynchronous, discounted) are only worth it for a very large backfill.

**Vector storage**

| Option | Notes |
|---|---|
| **pgvector in the existing PostgreSQL** | Transactional with the rest of the data; one backup; joins to hosts, jobs and policies for filtered search; HNSW indexes; iterative index scans for filtered queries since 0.8.0. Handles millions of vectors on one node. **Recommended.** |
| Meilisearch vectors (already deployed) | Supports user-provided vectors and hybrid keyword + semantic search. It was experimental in v1.11 (the version in `compose.yaml`) and is reported as generally available from v1.13, so it needs an upgrade. It is a derived index, not a source of truth. Good *later* addition for hybrid search in the dashboard. |
| Dedicated vector database (Qdrant, Weaviate, Milvus) | New infrastructure to deploy, secure and back up. Not justified at this scale and against the project's "one database" direction. Rejected. |

What pgvector needs in this repo:

- Image `pgvector/pgvector:pg16` in `compose.yaml` and in the test fixtures (`PostgresFixture.cs:12`, `EndToEnd/InfrastructureFixture.cs`). **Caution:** the current image is Alpine (musl) and the pgvector image is Debian (glibc). Text collation order differs between them, so an existing data volume needs a `REINDEX` of text indexes, or a dump and restore.
- `CREATE EXTENSION IF NOT EXISTS vector` in the migration (needs sufficient database rights).
- Npgsql mapping: packages `Pgvector` 0.3.2 and `Pgvector.Dapper` 0.3.1 (last published in 2025), with `UseVector()` on an `NpgsqlDataSourceBuilder` where the data source is created (`PostgresServiceCollectionExtensions.cs:21`). A dependency-free alternative is to send the vector as a text literal and cast it (`@Embedding::vector`).
- Up to 2,000 dimensions for an HNSW index on `vector`; `halfvec` halves the storage if size becomes a concern.

### 7.4 LLM analysis

**Shape.** One request per page: a fixed system prompt (instructions and output schema) and the page Markdown as input; the response is JSON that matches a schema (summary, category from a fixed list, entities, language, ...). The step validates the JSON and stores it as an inline artifact.

**Provider and SDK.** The official C# SDK is the `Anthropic` package (13.8.0); it also offers an `IChatClient` integration. Use structured outputs (`OutputConfig.Format` with a JSON schema) rather than a forced tool call: current Claude models reject forced `tool_choice`. Thinking cannot be disabled on the current Opus and Sonnet models; depth is set with `OutputConfig.Effort`, and `low` is appropriate for summarising and classifying. The step must check the stop reason before reading the content: `refusal` becomes `Skipped("refusal")`, `max_tokens` becomes `Failed("truncated")`.

**Model choice is an owner decision**, because it sets the cost by two orders of magnitude. List prices per million tokens (input / output), from Anthropic's reference dated 2026-11-03:

| Model | Price | Rough cost per 100,000 pages (3,000 input + 300 output tokens each, without thinking tokens) |
|---|---|---|
| `claude-opus-5-5` | $4 / $20 | about $1,800 (about $900 with the Batch API) |
| `claude-sonnet-5-5` | $2 / $10 | about $900 (about $450) |
| `claude-haiku-5-5` | $0.10 / $0.50 | about $45 (about $23) |

The model id is configuration and part of the step's `ConfigurationFingerprint`. Summary, classification and entity extraction are the kind of work the small model is built for, but the right way to choose is to run a sample of real pages through two models and compare. The estimates exclude thinking tokens, which are billed as output.

**Cost controls, in order of effect**

1. **Do not call at all.** Eligibility rule (5.8), minimum and maximum length, and hash-based skipping (5.4).
2. **No automatic re-runs.** `RerunPolicy.Manual`; a prompt change shows the number of stale pages and an estimate, and a person starts the re-run.
3. **Budget.** A daily token or money limit per step. The handler sums today's `Metrics` before each batch and returns 0 when the limit is reached; the summary endpoint shows it.
4. **Message Batches API.** 50 % off, results within 24 hours, no tool loop (not needed here). Flow: a submit handler claims up to N pending tasks, creates one provider batch with `custom_id = task id`, stores a `ProcessingBatches` row and sets the tasks to `Submitted`. A collect handler polls open batches and completes each task from its result; results arrive in any order and are matched by `custom_id`.
5. **Prompt caching.** Put the stable instructions and schema first and the page text last. The minimum cacheable prefix is 512 tokens on the current models, and inside a batch cache hits are best-effort, so the gain is real but smaller than items 1–4.
6. **Output limit.** A tight schema and a modest `max_tokens`.

Pages longer than the limit must not be silently truncated: either skip with a reason or summarise per section and combine. This needs a decision (open question 8).

**Security.** Page text is untrusted input and may contain instructions aimed at the model. The analysis request must have no tools, the output must be schema-constrained and stored as data, and nothing downstream may act on it automatically.

## 8. Recommended plan

### Phase 0: decisions (no code)

Answer the open questions in section 11 that block phase 1: questions 1 to 4.

### Phase 1: framework and Markdown, end to end

1. Migration `027_CreateProcessingPipeline`: `ProcessingTasks`, `ProcessingArtifacts`, `ParsedDocuments.PipelinePlannedAt`; table name constants in both `TableNames` files.
2. `Core/Processing`: `IProcessingStep`, `StepDescriptor`, `StepContext`, `StepResult` types, `StepRegistry` (with validation), `StepWorkHandler<TStep>`, `PipelinePlanHandler`, eligibility policy, input hashing, `AddProcessingStep<TStep>`.
3. `Core/Ports`: `IProcessingTaskStore`, `IArtifactStore`, `IMarkdownConverter`, `IProcessingReader` (read model).
4. `Adapters`: `PostgresProcessingTaskStore` (claim, complete, requeue, sweep), `PostgresProcessingReader`, `MinioArtifactStore`, `ReadableMarkdownConverter` (extractor chain + ReverseMarkdown + clean-up); packages `ReverseMarkdown` and `SmartReader` in `Directory.Packages.props`.
5. `Core/Processing/Steps/MarkdownStep`.
6. Worker: hosted services for `PipelinePlanHandler` and `StepWorkHandler<MarkdownStep>`; options section `Processing`.
7. API: page processing status, artifact content, re-run for one page and for a scope, summary.
8. Frontend: "Processing" card and "Markdown" tab; mock data for MSW.
9. One line in the `ParsedDocuments` upsert to reset `PipelinePlannedAt` on a re-parse.
10. Tests: unit tests for registry validation, hashing, result transitions, eligibility and the Markdown clean-up functions; golden-file tests for the converter; integration tests (Testcontainers) for claim, lease takeover, attempts, requeue; an end-to-end test from a stored fetch to a Markdown artifact. Update `ServiceRegistrationTests`.

Exit criteria: a crawled documentation page shows clean Markdown in the dashboard; a failing page shows its reason; "re-run" works; a recrawl with unchanged content keeps the same `OutputHash`.

### Phase 2: chunking, embeddings, semantic search

pgvector image and extension; `PageChunks`; `ChunkStep` and `EmbedStep`; `ITextEmbedder` adapter for the chosen provider with rate limiting; `ISemanticSearch` port and `GET /api/v1/search/semantic`; MCP tools for Markdown and semantic search.

### Phase 3: LLM analysis

`ILanguageModel` adapter; `AnalyzeStep` with a schema and prompt in `Core`; budget; dry-run estimates; synchronous mode first on a small scope, then `ProcessingBatches` and the submit and collect handlers.

### Phase 4: operations and search quality

Worker roles and a separate processing container; per-step counters as events; `UrlFetches.ContentHash`; summary and category fields in the Meilisearch document; hybrid search after a Meilisearch upgrade; artifact garbage collection.

## 9. Alternatives considered

| Alternative | Why not |
|---|---|
| **Extend today's pattern**: a timestamp column per stage (`MarkdownAt`, `EmbeddedAt`, ...) with one backlog port and SQL per stage | Consistent with parse and index, but every new step needs a migration, a new port, new SQL and a `ContentStates` change. No attempts, failure reasons or versions. This is the opposite of open/closed. |
| **Do the Markdown conversion inside `ParsePageHandler`** | Fewest moving parts, but a conversion bug would block link discovery, the step could not be re-run or versioned on its own, and it would not generalise to the paid steps. |
| **A job library on PostgreSQL** (Hangfire with PostgreSQL storage, Quartz.NET, Wolverine, TickerQ) | They provide retries and a dashboard quickly. But they bring their own schema, serialisation and UI next to an existing, tested lease idiom; the per-page per-step status, hashing and artifact model would still have to be built; and jobs become opaque rows instead of queryable domain data. The project removed its previous queue technology on purpose. |
| **A message broker** (RabbitMQ, Redis Streams, Kafka) | New infrastructure; removed from this project before (`docs/refactoring-plan.md` §1.1). |
| **A workflow engine** (Temporal and similar) | Powerful for long workflows, but a new server and a new programming model for a four-step pipeline. |
| **`Microsoft.Extensions.DataIngestion` pipeline** | Reader → chunker → writer in one process call; not durable per step, no per-step status; preview packages only. |
| **A Python sidecar for all processing** | The best library ecosystem for extraction and embeddings, but a second runtime, a second deployment and a network hop. The ports keep this possible for a single step later. |
| **Linear pipeline instead of a DAG** | Simpler to reason about, but couples the independent embedding and analysis branches. |
| **Tasks keyed by fetch instead of by page** | Would keep history, but the raw object of older fetches no longer exists (finding 2.6.1), and the table would grow with every recrawl. |
| **Dedicated vector database** | See 7.3. |

## 10. Risks

- **Markdown quality varies by site.** Mitigation: extractor chain, quality score in `Metrics`, golden files, automatic re-run on a version bump (the step is free).
- **Table growth.** `ProcessingTasks` has (pages × steps) rows and `PageChunks` about 5–20 rows per page with a 4 KB vector each (1,024 floats). One million pages would mean roughly 40–80 GB of vectors; `halfvec` halves it. Know the expected corpus size before phase 2.
- **Polling load.** Four more loops poll once per second when idle. The partial index keeps each poll cheap; the idle delay can be raised per step.
- **Double execution of paid calls** when a lease expires. Mitigation: generous leases, lease check on completion, low concurrency.
- **Orphaned objects** in storage when pages are deleted or artifacts are replaced. Raw HTML has the same property today. A periodic clean-up belongs to phase 4.
- **PostgreSQL image change** for pgvector (collation, see 7.3).
- **Package version alignment.** `Microsoft.Extensions.AI` is at 11.0.0 while the repo pins `Microsoft.Extensions.*` 10.0.x with transitive pinning; the matching 10.x line should be used unless the repo moves to 11.

## 11. Open questions for the owner

1. **Scope of processing.** All eligible pages of all jobs, or opt-in per job or per host? Should pages with `noindex` be processed? (Recommendation: follow the index policy.)
2. **Markdown storage.** Object storage (recommended) or a PostgreSQL `text` column?
3. **Per-item exception handling.** Is a guarded catch in `StepWorkHandler` acceptable, given the rule that only `PollingWorker` catches `Exception`?
4. **Duplicates across jobs.** The same URL in two jobs is two pages and would be processed twice. Accept this, or share results by content hash?
5. **May page content be sent to external providers** (embeddings, LLM)? This decides hosted versus local models.
6. **Languages.** Is the content multilingual? This affects the embedding model and the analysis prompt.
7. **Expected corpus size and recrawl frequency.** Needed for storage sizing, rate limits and budgets.
8. **LLM analysis output.** Which fields exactly (summary length, category list, entity types)? What should happen with pages that are too long? Which model tier, and what monthly budget?
9. **What consumes the results?** Dashboard only, the MCP server, an external RAG application? This decides the search API of phase 2 and whether hybrid search in Meilisearch is wanted.
10. **Thin pages.** Should a page that looks like an empty JavaScript shell be re-fetched automatically with rendering?
11. **History.** Is keeping only the latest Markdown per page enough, or are older versions needed (which would also require fixing the overwrite of raw HTML)?
12. **Non-HTML documents.** Only HTML is stored today. Are PDFs in scope later? The step design allows a second converter, but the crawler would have to store them first.

## 12. What I could not verify

- **Nothing was built or run.** The interfaces and SQL in this report are sketches. They were written to fit the analyzer limits but have not been compiled.
- **pgvector's exact current version.** Search results disagree (0.8.2 or 0.8.3); the features used here (HNSW, `halfvec`, iterative scans) exist since 0.8.0. I also did not verify that `Pgvector` 0.3.2 / `Pgvector.Dapper` 0.3.1 work with Npgsql 10; the text-cast fallback avoids the question.
- **Embedding prices and Voyage model details** come from third-party pages, not from the providers' own price lists. The OpenAI embedding price was not checked at all; treat all embedding cost figures as estimates.
- **Meilisearch**: that vector search is generally available from v1.13 comes from a Meilisearch blog post; I did not check the changelog or the current server version.
- **LLM prices and API behaviour** come from Anthropic's reference material dated 2026-11-03, not from a live price page. The cost table excludes thinking tokens.
- **SmartReader and ReverseMarkdown output quality** on documentation pages was not tested; the statements about Readability's weaknesses on non-article pages are general experience, not measurements on this project's data.
- **Compatibility of `Microsoft.Extensions.AI` 11.0.0** with the repo's pinned 10.0.x extension packages was not tested.
- **The JS rendering setup** is itself documented as unverified end to end (`README.md`, section "JavaScript rendering").
- **The frontend** was only surveyed by file name; I did not read the page components in detail.
