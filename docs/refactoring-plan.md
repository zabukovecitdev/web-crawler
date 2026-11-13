# SamoBot refactoring plan

This plan changes SamoBot to the redesigned architecture. It also applies SOLID, DRY, OOP, and a method complexity rule.
A small model can do this plan. Each task is small, has exact file names, and has a check.

## 0. How to use this plan

1. Do one task at a time. Do the tasks in the given order.
2. Read section 2 (rules) before each task.
3. After each task, run the check commands in section 3. Do not start the next task if a check fails.
4. Do not change files that the task does not name.
5. If a task tells you to move a file, move it. Do not write it again.
6. If something in this plan does not agree with the code, stop. Report the difference. Do not guess.
7. Make one git commit for each task. Use the task ID in the message, for example `T2.3 Add CrawlOutcome types`.
8. Work on the branch `refactor/redesign`. Do not work on `main`.

## 1. Target design

### 1.1 Decisions that are already made

- PostgreSQL is the only queue. RabbitMQ, Redis, and Neo4j are removed.
- Two processes stay: `SamoBot.Worker` and `SamoBot.Api`.
- Each stage uses the same pattern: claim rows with a lease, do the work, record one result.
- The HTTP routes and the JSON shapes of `SamoBot.Api` do not change. The `frontend/` folder does not change.

### 1.2 Projects

| Project | Content | Can reference |
|---|---|---|
| `src/SamoBot.Core` | Domain types, use cases, port interfaces | No project. No I/O packages. |
| `src/SamoBot.Adapters` | PostgreSQL, HTTP, browser, MinIO, Meilisearch code | `Core` |
| `src/SamoBot.Worker` | Host for the background loops | `Core`, `Adapters` |
| `src/SamoBot.Api` | Dashboard API and SignalR | `Core`, `Adapters` |
| `src/SamoBot.Migrations` | FluentMigrator migrations | none |
| `SamoBot.Mcp` | MCP search server | `Core`, `Adapters` |
| `tests/SamoBot.Tests` | Unit tests and integration tests | all |

`SamoBot.Core` must not reference Npgsql, Dapper, Minio, Meilisearch, Playwright, or `Microsoft.Extensions.Hosting`.
The compiler then enforces the Dependency Inversion Principle.

### 1.3 Folders in `SamoBot.Core`

```
SamoBot.Core/
├── Domain/        CrawlUrl, ClaimedUrl, CrawlJob, FetchResponse, FetchRecord, ParsedPage,
│                  RobotsRules, UrlUpdate, CrawlOutcome (and its subclasses), enums
├── Crawling/      CrawlBatchHandler, CrawlUrlHandler, OutcomeClassifier, BackoffPolicy,
│                  FetcherResolver, RobotsGate
├── Parsing/       ParseBatchHandler, ParsePageHandler, LinkResolver, JobScope
├── Indexing/      IndexBatchHandler
├── Jobs/          CrawlJobService, JobCompletionHandler
├── Ports/         all interfaces from section 1.5
└── Common/        IWorkHandler, JsonDefaults, UrlNormalizer
```

### 1.4 Folders in `SamoBot.Adapters`

```
SamoBot.Adapters/
├── Postgres/      PostgresUrlFrontier, PostgresParseBacklog, PostgresIndexBacklog,
│                  PostgresCrawlJobRepository, PostgresJobPageReader, PostgresRobotsStore,
│                  PostgresCrawlEventPublisher, TableNames
├── Http/          HttpFetcher, HttpRobotsSource
├── Browser/       BrowserFetcher
├── Storage/       MinioContentStore, ObjectNameGenerator
├── Search/        MeilisearchIndex
├── Html/          HtmlAgilityPackParser, MetaRobotsValidator, HtmlContentValidator
└── AdaptersServiceCollectionExtensions.cs
```

### 1.5 Port interfaces (all in `SamoBot.Core/Ports`)

Each interface is small. A class depends only on the interface that it uses.

```csharp
public interface IUrlFrontier
{
    Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, CancellationToken ct);
    Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct);
    Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct);
}

public interface IParseBacklog
{
    Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct);
    Task Complete(FetchToParse fetch, ParsedPage page, IReadOnlyList<NewUrl> links, CancellationToken ct);
}

public interface IIndexBacklog
{
    Task<IReadOnlyList<PageToIndex>> Claim(int batchSize, CancellationToken ct);
    Task MarkIndexed(IReadOnlyList<int> pageIds, CancellationToken ct);
}

public interface IFetcher
{
    bool Supports(ClaimedUrl url);
    Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct);
}

public interface IContentStore
{
    Task<string> Save(string url, byte[] content, string? contentType, CancellationToken ct);
    Task<Stream> Load(string objectName, CancellationToken ct);
}

public interface IHtmlParser      { ParsedPage Parse(Stream html); }
public interface ISearchIndex     { Task AddOrUpdate(IReadOnlyList<PageToIndex> pages, CancellationToken ct); }
public interface ISearchQuery     { Task<IReadOnlyList<SearchHit>> Search(string text, int limit, CancellationToken ct); }
public interface IRobotsStore     { Task<RobotsRules?> Find(string host, CancellationToken ct); Task Save(RobotsRules rules, CancellationToken ct); }
public interface IRobotsSource    { Task<RobotsRules> Download(string host, CancellationToken ct); }
public interface ICrawlEventPublisher { Task Publish(int crawlJobId, string eventType, object payload, CancellationToken ct); }
public interface ICrawlJobRepository  { /* Find, ListRecent, Insert, Save, CompleteFinishedJobs */ }
public interface IJobPageReader       { /* ListPages, CountPages, ListEvents */ }
```

### 1.6 The shared worker loop

One interface and one generic class replace the eight worker classes.

```csharp
// SamoBot.Core/Common/IWorkHandler.cs
public interface IWorkHandler
{
    /// <summary>Processes one batch. Returns the number of processed items.</summary>
    Task<int> HandleBatch(CancellationToken ct);
}

// SamoBot.Worker/PollingWorker.cs
public sealed class PollingWorker<THandler> : BackgroundService where THandler : IWorkHandler
{
    // Loop: create a DI scope, resolve THandler, call HandleBatch.
    // If the result is 0, wait 1 second.
    // If HandleBatch throws, log the exception and wait 5 seconds.
    // This is the only place in the Worker that catches Exception.
}
```

Handlers: `CrawlBatchHandler`, `ParseBatchHandler`, `IndexBatchHandler`, `JobCompletionHandler`.

### 1.7 Crawl outcomes use polymorphism

Do not write `if` chains on status codes in more than one place.
`OutcomeClassifier` makes one `CrawlOutcome` object. The object knows the new URL state.

```csharp
public abstract record CrawlOutcome
{
    public abstract UrlUpdate ToUpdate(ClaimedUrl url, DateTimeOffset now);
}
public sealed record Succeeded(TimeSpan RecrawlAfter) : CrawlOutcome { /* Idle, NextCrawlAt = now + RecrawlAfter, FailCount = 0 */ }
public sealed record RetryLater(string Reason, TimeSpan Delay) : CrawlOutcome { /* Idle, FailCount + 1, NextCrawlAt = now + Delay */ }
public sealed record Disabled(string Reason) : CrawlOutcome { /* Disabled, FailReason = Reason */ }
public sealed record Redirected(string TargetUrl) : CrawlOutcome { /* Disabled, FailReason = "redirect", NewUrl = TargetUrl */ }
```

`UrlUpdate` is a record: `Status`, `NextCrawlAt`, `FailCount`, `FailReason`, `NewUrl`.
`PostgresUrlFrontier.Complete` writes the `UrlUpdate` fields. It has no `if` on the outcome type.

Classification table for `OutcomeClassifier`:

| Input | Outcome |
|---|---|
| robots.txt blocks the URL | `Disabled("robots")` |
| Status 200 to 299 | `Succeeded(1 day)` |
| Status 301, 302, 303, 307, 308 with a `Location` | `Redirected(location)` |
| Status 429, 500 to 599, or 0 (network error), and `FailCount + 1 < 5` | `RetryLater(reason, BackoffPolicy.DelayFor(failCount))` |
| The same as the line before, and `FailCount + 1 >= 5` | `Disabled("max-failures")` |
| All other status codes | `Disabled("http-" + statusCode)` |

## 2. Rules for all code

### 2.1 Limits (the build enforces them)

| Rule | Limit | Analyzer rule |
|---|---|---|
| Cyclomatic complexity of a method | 8 | CA1502 |
| Cognitive complexity of a method | 10 | S3776 |
| Lines in a method | 30 | S138 |
| Parameters of a method or constructor | 6 | S107 |
| Nesting depth of control blocks | 2 | S134 |
| Lines in a file | 300 | S104 |
| Braces on `if`, `for`, `foreach`, `while` | always | IDE0011 |

If a method breaks a limit, extract a private method or a new class. Do not suppress the rule.
`#pragma warning disable` and `[SuppressMessage]` are not permitted.

### 2.2 SOLID

- **S.** A class has one reason to change. A class name must not contain `Manager`, `Helper`, `Util`, or `And`.
- **O.** To add a behavior, add a class that implements an existing interface. Example: a new `IFetcher`.
- **L.** Each implementation obeys the interface contract. A fetcher never returns `null`. A fetcher never throws for an HTTP error status.
- **I.** An interface has 5 methods or fewer. Do not add a generic `IRepository<T>`.
- **D.** Classes in `Core` depend on `Ports` interfaces only. Only `Program.cs` and `*ServiceCollectionExtensions.cs` use `new` for a service class.

### 2.3 DRY

- One place for each piece of knowledge. Before you add a helper, search the solution for the same logic.
- Use `JsonDefaults.Options` for all JSON. Do not create a second `JsonSerializerOptions`.
- Use `TableNames` constants in SQL strings in `Adapters`.
- Package versions are only in `Directory.Packages.props`.
- Build settings are only in `Directory.Build.props`.

### 2.4 OOP and style

- Classes are `sealed` unless this plan makes them `abstract`.
- Use constructor injection. Do not inject `IServiceProvider`, except in `PollingWorker`.
- Domain types hold their behavior. Example: `CrawlJob.Pause()` checks the status. The service does not.
- Use `FluentResults` `Result` for expected failures, for example an invalid state transition.
- Use exceptions for infrastructure failures. Do not catch `Exception`, except in `PollingWorker`.
- Do not write `async void`. Pass the `CancellationToken` to each async call.
- Use `TimeProvider` for the current time. Do not call `DateTime.UtcNow` or `DateTimeOffset.UtcNow`.
- Use the namespace `SamoBot`. The spelling `Samobot` is wrong.
- Repositories are stateless. Each method opens a connection: `await using var conn = await dataSource.OpenConnectionAsync(ct);`.
- Write SQL as `const string` with Dapper. Do not use SqlKata.

## 3. Check commands

Run these after each task:

```bash
dotnet build SamoBot.sln -warnaserror
dotnet test tests/SamoBot.Tests --filter "Category!=Integration"
```

Run this after each task in phases 4 to 8 (Docker is necessary):

```bash
dotnet test tests/SamoBot.Tests --filter "Category=Integration"
```

## Phase 0. Guardrails

**T0.1 Create the branch.** Run `git switch -c refactor/redesign`.

**T0.2 Add `Directory.Build.props` in the repository root.**
Set `TargetFramework` `net10.0`, `Nullable` `enable`, `ImplicitUsings` `enable`, `EnforceCodeStyleInBuild` `true`, `AnalysisLevel` `latest-recommended`.
Add `SonarAnalyzer.CSharp` as a `PackageReference` with `PrivateAssets="all"`.
Add `CodeMetricsConfig.txt` and `SonarLint.xml` as `AdditionalFiles`.
Remove the same properties from each `.csproj`.

**T0.3 Add `Directory.Packages.props`.**
Set `ManagePackageVersionsCentrally` to `true`. Move each package version from the `.csproj` files to this file.

**T0.4 Add `CodeMetricsConfig.txt`** with one line: `CA1502(Method): 8`.

**T0.5 Add `SonarLint.xml`.** Set these parameters: S3776 `threshold` 10, S138 `max` 30, S107 `max` 6, S134 `max` 2, S104 `maximumFileLocThreshold` 300.
Read the SonarAnalyzer documentation for each rule and confirm the parameter name. The names in this task could be wrong.

**T0.6 Add `.editorconfig`.**
Set `csharp_prefer_braces = true`. Set the severity of IDE0011, CA1502, S3776, S138, S107, S134, and S104 to `error`.
In the section `[tests/**.cs]`, set S138 and S104 to `none`.

**T0.7 Keep the old projects building.**
In each old project (`SamoBot`, `SamoBot.Infrastructure`, `SamoBot.Workers`, `SamoBot.Crawler`, `SamoBot.Scheduler`, `SamoBot.Parser`, `SamoBot.Indexer`, `SamoBot.Seeder`, `SamoBot.Settings`, `SamoBot.Api`, `SamoBot.Mcp`), add `<TreatWarningsAsErrors>false</TreatWarningsAsErrors>` and `<AnalysisMode>None</AnalysisMode>`.
Later tasks delete these projects or remove these two lines.

**T0.8 Prove that the limits work.**
Create the empty projects `src/SamoBot.Core` and `src/SamoBot.Adapters`. Add them to `SamoBot.sln`.
In `SamoBot.Core`, add a temporary method with 12 `if` statements and 40 lines.
Check: the build fails with CA1502, S3776, and S138. Then delete the temporary method.
If a rule does not fail the build, correct the configuration before you continue.

**T0.9 Delete stale files.**
Delete `ARCHITECTURE.md`, `PROCESS.md`, `IMPLEMENTATION_PLAN.md`, `docker-compose.yml`, `SamoBot.sln.DotSettings.user`, and `docs/superpowers/`.
Add `*.DotSettings.user` to `.gitignore`.

## Phase 1. Domain (`SamoBot.Core/Domain`)

Each task in this phase includes unit tests in `tests/SamoBot.Tests/Domain`.

**T1.1 Move `UrlNormalizer`.**
Move `src/SamoBot.Infrastructure/Utilities/UrlNormalizer.cs` to `src/SamoBot.Core/Common/`. Change only the namespace.
Move `UrlExtensions.cs` with it if `UrlNormalizer` uses it.
Update `tests/SamoBot.Tests/UrlNormalizerTests.cs` to the new namespace. All 77 tests must pass.
If `UrlNormalizer` breaks a limit, extract private methods. Do not change the behavior.

**T1.2 Add the enums.** `UrlStatus` (`Idle`, `InFlight`, `Disabled`) and `CrawlJobStatus` (copy the values from the old enum).

**T1.3 Add the records.** `NewUrl`, `ClaimedUrl`, `FetchResponse`, `FetchRecord`, `FetchToParse`, `ParsedPage`, `ParsedLink`, `PageToIndex`, `SearchHit`, `UrlUpdate`.
Copy the property names from the old models in `src/SamoBot.Infrastructure/Models`.
`FetchResponse` has: `StatusCode`, `ContentType`, `Content`, `ResponseTimeMs`, `RedirectLocation`, `Error`.

**T1.4 Add `CrawlOutcome` and its four subclasses** from section 1.7. Test `ToUpdate` for each subclass.

**T1.5 Add `CrawlJob` with behavior.**
Methods: `Result Start(DateTimeOffset now)`, `Result Pause(DateTimeOffset now)`, `Result Cancel(DateTimeOffset now)`, `void Complete(DateTimeOffset now)`.
Each method checks the current status and returns `Result.Fail` for an invalid transition.
Permitted transitions: `Pending` or `Paused` to `Running`. `Running` to `Paused`. All states except `Completed` to `Cancelled`.
Add the property `AllowedHosts`.

**T1.6 Add `RobotsRules`.**
It holds `Host`, `Content`, `FetchedAt`, `ExpiresAt`, and `CrawlDelay`.
It parses `Content` one time in the constructor with the `RobotsTxtParser` package.
Methods: `bool IsAllowed(string pathAndQuery, string userAgent)` and `static RobotsRules AllowAll(...)` and `static RobotsRules DisallowAll(...)`.

## Phase 2. Use cases (`SamoBot.Core`)

Each task includes unit tests. Use hand-written fake classes for the ports in `tests/SamoBot.Tests/Fakes`.

**T2.1 Add all interfaces from section 1.5** to `SamoBot.Core/Ports`. Add `IWorkHandler` and `JsonDefaults` to `Common`.

**T2.2 Add `BackoffPolicy`.** `TimeSpan DelayFor(int failCount)` returns `1 minute * 2^failCount`, with a maximum of 1 hour.

**T2.3 Add `OutcomeClassifier`.** `CrawlOutcome Classify(FetchResponse response, int failCount)`. Use the table in section 1.7. Write one test for each table row.

**T2.4 Add `RobotsGate`.**
`Task<bool> IsAllowed(ClaimedUrl url, CancellationToken ct)`.
It returns `true` if `url.RespectRobots` is `false`.
It gets `RobotsRules` in this order: `IMemoryCache`, `IRobotsStore`, `IRobotsSource`. It saves downloaded rules to the store and the cache.
The user agent is the constant `SamoBot`.

**T2.5 Add `FetcherResolver`.** It gets `IEnumerable<IFetcher>`. `IFetcher For(ClaimedUrl url)` returns the first fetcher where `Supports(url)` is `true`.

**T2.6 Add `CrawlUrlHandler`.** `Task Handle(ClaimedUrl url, CancellationToken ct)`. Each step is one private method:
1. If `RobotsGate.IsAllowed` is `false`, complete the URL with `Disabled("robots")` and stop.
2. Fetch with `FetcherResolver.For(url).Fetch`.
3. If the response is HTML and has content, save it with `IContentStore.Save`.
4. Classify with `OutcomeClassifier`.
5. Call `IUrlFrontier.Complete` with the update and the `FetchRecord`.
6. Publish the event `FetchCompleted` with `ICrawlEventPublisher`.

**T2.7 Add `CrawlBatchHandler : IWorkHandler`.** It claims 20 URLs with a 5 minute lease. It runs `CrawlUrlHandler.Handle` for all URLs with `Task.WhenAll`. It returns the count.

**T2.8 Add `LinkResolver`.**
`IReadOnlyList<string> Resolve(Uri pageUrl, IEnumerable<ParsedLink> links)`.
Always use `new Uri(pageUrl, link.Url)`. Do not call `Uri.TryCreate(link, UriKind.Absolute)` first. On Linux, that call changes `/path` to a `file://` URI.
Keep only `http` and `https` results. Normalize each result with `UrlNormalizer`. Remove duplicates.
Tests: `/about` on `https://a.example/x` gives `https://a.example/about`. `mailto:` links are removed.

**T2.9 Add `JobScope`.** `IReadOnlyList<NewUrl> Filter(FetchToParse parent, IReadOnlyList<string> urls)`. It removes URLs whose host is not in `AllowedHosts`. It returns an empty list if `parent.Depth + 1 > MaxDepth`.

**T2.10 Add `ParsePageHandler` and `ParseBatchHandler : IWorkHandler`.**
Steps for one fetch: load the HTML with `IContentStore.Load`, parse with `IHtmlParser`, resolve links with `LinkResolver`, filter with `JobScope`, call `IParseBacklog.Complete`.
If the page has a `nofollow` directive, pass an empty link list.
The batch handler claims 10 fetches with a 5 minute lease.

**T2.11 Add `IndexBatchHandler : IWorkHandler`.** It claims 50 pages, calls `ISearchIndex.AddOrUpdate` one time, then calls `IIndexBacklog.MarkIndexed`.

**T2.12 Add `CrawlJobService` and `JobCompletionHandler : IWorkHandler`.**
`CrawlJobService` has `Create`, `Start`, `Pause`, `Cancel`. Each method loads the job, calls the `CrawlJob` method, saves, and publishes one event.
`Start` also calls `IUrlFrontier.AddSeeds`.
`JobCompletionHandler` calls `ICrawlJobRepository.CompleteFinishedJobs` and returns 0.

## Phase 3. Database

**T3.1 Add migration `014_RedesignFrontier.cs`** in `src/SamoBot.Migrations/Migrations`.
- `DiscoveredUrls`: add `LeaseUntil` (nullable timestamp with time zone) and `FailReason` (nullable text).
- `DiscoveredUrls`: delete the unique constraint `IX_DiscoveredUrls_NormalizedUrl_Unique`. Add a unique constraint on (`CrawlJobId`, `NormalizedUrl`).
- `DiscoveredUrls`: add an index on (`Host`, `Status`, `NextCrawlAt`).
- Create the table `Hosts`: `Host` (text, primary key), `NextAllowedAt` (timestamp with time zone, default `now()`), `CrawlDelayMs` (integer, default 1000).
- `UrlFetches`: add `LeaseUntil` and `FinalUrl`.
- `ParsedDocuments`: add `IndexedAt` (nullable). Set `IndexedAt = now()` for rows that have a `Completed` row in `IndexJobs`.
- `CrawlJobs`: add `AllowedHosts` (text, JSON array).
- Delete the table `IndexJobs`.
- Write the `Down` method.

**T3.2 Remove the project reference** from `SamoBot.Migrations` to `SamoBot`. Keep the local `TableNames` file in the migrations project.

## Phase 4. Adapters (`SamoBot.Adapters`)

Each PostgreSQL task includes an integration test with Testcontainers. Mark these tests with `[Trait("Category", "Integration")]`.
Add one shared test fixture that starts PostgreSQL and runs the migrations.

**T4.1 Add `TableNames` and the `NpgsqlDataSource` registration.**

**T4.2 Add `PostgresUrlFrontier`.**
- `Claim`: use the claim query in section 4.1. Tests: two parallel `Claim` calls return no common URL. A host gives one URL per call. A URL of a `Paused` job is not returned. An expired lease is claimed again.
- `Complete`: in one transaction, insert the `FetchRecord` (if not null), update the URL from `UrlUpdate`, and insert `UrlUpdate.NewUrl` (if not null) with `ON CONFLICT DO NOTHING`.
- `AddSeeds`: insert with `ON CONFLICT DO NOTHING`. Insert each new host into `Hosts` with `ON CONFLICT DO NOTHING`.

**T4.3 Add `PostgresParseBacklog`.**
- `Claim`: select `UrlFetches` rows where `ParsedAt` is null, `ObjectName` is not null, and the lease is null or expired. Use `FOR UPDATE SKIP LOCKED` in raw SQL.
- `Complete`: in one transaction, upsert `ParsedDocuments` on `UrlFetchId`, insert the links and their hosts with `ON CONFLICT DO NOTHING`, and set `ParsedAt`. Apply `MaxUrls` in the insert.

**T4.4 Add `PostgresIndexBacklog`, `PostgresCrawlJobRepository`, `PostgresJobPageReader`, `PostgresRobotsStore`.**
`CompleteFinishedJobs` sets `Completed` for `Running` jobs that have no `InFlight` URL and no `Idle` URL with a null `LastCrawlAt`.
`PostgresRobotsStore.Save` uses `INSERT ... ON CONFLICT ("Host") DO UPDATE`.

**T4.5 Add `PostgresCrawlEventPublisher`.** Insert the event row. Then run `SELECT pg_notify('crawl_events', @payload)` on the same connection.

**T4.6 Add `HttpFetcher : IFetcher`.**
- `Supports` returns `true` when `url.UseJsRendering` is `false` or browser rendering is disabled.
- User agent: `SamoBot/1.0 (+https://github.com/<owner>/web-crawler)`. Ask the user for the URL.
- Set `AllowAutoRedirect = false`. Put the `Location` header in `FetchResponse.RedirectLocation`.
- Timeout: 30 seconds. Maximum body size: 5 MB. Stop reading after the limit.
- A network error or timeout returns `StatusCode = 0` and `Error`. It does not throw.
- Do not use Polly. The frontier does the retries.

**T4.7 Add `BrowserFetcher : IFetcher`.** Move the Playwright code from `JsRenderService.cs`. `Supports` returns `true` when `url.UseJsRendering` is `true` and rendering is enabled. On a render error, return `StatusCode = 0` and `Error`.

**T4.8 Add `HttpRobotsSource : IRobotsSource`.**
Status 200 to 299: parse the body, expire in 24 hours. Status 400 to 499: `RobotsRules.AllowAll`, expire in 24 hours. Status 500 to 599 or network error: `RobotsRules.DisallowAll`, expire in 1 hour.

**T4.9 Move the storage, HTML, and search classes.**
- `MinioHtmlUploader.cs` becomes `Storage/MinioContentStore.cs` and implements `IContentStore`. Register `IMinioClient` as a singleton.
- Move `ObjectNameGenerator.cs`, `HtmlContentValidator.cs`, `MetaRobotsValidator.cs`.
- `HtmlParser.cs` becomes `Html/HtmlAgilityPackParser.cs` and implements `IHtmlParser`. Split methods that break a limit.
- `IndexService.cs` and `SearchService.cs` become `Search/MeilisearchIndex.cs`. It implements `ISearchIndex` and `ISearchQuery`.

**T4.10 Add `AdaptersServiceCollectionExtensions`.** One method `AddSamoBotAdapters(IConfiguration)` registers all adapters. Add `AddSamoBotCore()` in `SamoBot.Core` for the handlers and services. Each registration method must stay under 30 lines. Split by area if necessary.

### 4.1 Claim query for T4.2

This SQL was not run. Test it in T4.2 and correct it if a test fails.

```sql
WITH due_hosts AS (
    SELECT h."Host" FROM "Hosts" h
    WHERE h."NextAllowedAt" <= now()
      AND EXISTS (SELECT 1 FROM "DiscoveredUrls" u WHERE u."Host" = h."Host" AND u."Status" <> 'Disabled')
    ORDER BY h."NextAllowedAt"
    LIMIT @BatchSize
    FOR UPDATE SKIP LOCKED
),
picked AS (
    SELECT DISTINCT ON (u."Host") u."Id", u."Host"
    FROM "DiscoveredUrls" u
    JOIN due_hosts d ON d."Host" = u."Host"
    JOIN "CrawlJobs" j ON j."Id" = u."CrawlJobId" AND j."Status" = 'Running'
    WHERE (u."Status" = 'Idle' AND coalesce(u."NextCrawlAt", now()) <= now())
       OR (u."Status" = 'InFlight' AND u."LeaseUntil" < now())
    ORDER BY u."Host", u."Depth", u."Id"
),
bump AS (
    UPDATE "Hosts" h SET "NextAllowedAt" = now() + (h."CrawlDelayMs" * interval '1 millisecond')
    FROM picked p WHERE p."Host" = h."Host"
)
UPDATE "DiscoveredUrls" u
SET "Status" = 'InFlight', "LeaseUntil" = now() + @Lease
FROM picked p WHERE p."Id" = u."Id"
RETURNING u.*;
```

## Phase 5. Worker host

**T5.1 Create `src/SamoBot.Worker`.** Add `PollingWorker<THandler>` from section 1.6. Add a unit test: a handler that throws does not stop the loop.

**T5.2 Add `Program.cs`.** It calls `AddSamoBotCore()`, `AddSamoBotAdapters(configuration)`, and registers four hosted services:
`PollingWorker<CrawlBatchHandler>`, `PollingWorker<ParseBatchHandler>`, `PollingWorker<IndexBatchHandler>`, `PollingWorker<JobCompletionHandler>`.

**T5.3 Add `appsettings.json`** to `SamoBot.Worker`. Copy only these sections from `src/SamoBot.Settings/appsettings.json`: `Database`, `MinIO`, `Meilisearch`, `Crawler`, `ChromeRendering`, `Logging`.

**T5.4 Add `src/SamoBot.Worker/Dockerfile`.** Copy `src/SamoBot.Crawler/Dockerfile` and change the project name.

## Phase 6. API and MCP

**T6.1 Change `SamoBot.Api` to reference `Core` and `Adapters`.** Controllers use `CrawlJobService`, `IJobPageReader`, and `RobotsGate`. Do not change a route or a JSON property name.

**T6.2 Replace `CrawlTelemetryRedisSubscriber`** with `CrawlEventListener`. It runs `LISTEN crawl_events` on one Npgsql connection and sends each notification to the SignalR group.

**T6.3 Add `appsettings.json` and a `Dockerfile` to `SamoBot.Api`.** Remove the dev API key from the file. Read the key from the environment variable `DashboardApi__Key`.

**T6.4 Change `SamoBot.Mcp`** to use `ISearchQuery` from `Core`.

**T6.5 Remove the two lines from T0.7** in `SamoBot.Api` and `SamoBot.Mcp`. Correct each analyzer error.

## Phase 7. Delete the old system

**T7.1 Delete these projects** and remove them from `SamoBot.sln`:
`src/SamoBot`, `src/SamoBot.Infrastructure`, `src/SamoBot.Workers`, `src/SamoBot.Crawler`, `src/SamoBot.Scheduler`, `src/SamoBot.Parser`, `src/SamoBot.Indexer`, `src/SamoBot.Seeder`, `src/SamoBot.Settings`.
Delete `tests/SamoBot.Tests/PolitenessPolicyTests.cs` and `RedisCachePolitenessTests.cs`. Delete `terraform/` and `.run/`.

**T7.2 Remove unused packages** from `Directory.Packages.props`: `RabbitMQ.Client`, `StackExchange.Redis`, `Neo4j.Driver`, `Confluent.Kafka`, `SqlKata`, `SqlKata.Execution`, `Riok.Mapperly`, `Polly`, `Polly.Contrib.WaitAndRetry`, `Polly.Extensions.Http`, `Microsoft.Extensions.Http.Polly`, `RobotsTxtCore`, `Bogus`.

**T7.3 Rewrite `compose.yaml`.** Services: `postgres`, `minio`, `meilisearch`, `migrations`, `worker`, `api`. `worker` and `api` depend on `migrations` with `condition: service_completed_successfully`. Remove all profiles.

**T7.4 Update `.github/workflows/docker-publish.yml`.** The image matrix is: `samobot-worker`, `samobot-api`, `samobot-migrations`. Add a job that runs the check commands from section 3.

**T7.5 Write `README.md`.** Content: one diagram, how to run, the claim pattern, the design decisions, the limits table from section 2.1.

## Phase 8. Proof

**T8.1 Add the end-to-end test** `tests/SamoBot.Tests/EndToEnd/CrawlJobTests.cs` with category `Integration`.
Start PostgreSQL, MinIO, and Meilisearch with Testcontainers.
Start a stub site in the test with about 20 pages. It has a `robots.txt` with one `Disallow` path, one page that returns 500, one redirect, one root-relative link, and two pages that link to each other.
Create and start a job. Run the four handlers in a loop until the job is `Completed`, with a 60 second timeout.
Assert:
1. Each allowed page has exactly one successful fetch.
2. The disallowed page has no fetch and is `Disabled` with the reason `robots`.
3. The 500 page is `Disabled` with the reason `max-failures`.
4. The page behind the root-relative link is fetched.
5. Two fetches to one host are 1 second apart or more.
6. Each fetched HTML page is in the search index.

**T8.2 Add the crash test.** Claim a batch and do not complete it. Use a `FakeTimeProvider` or a 1 second lease. Assert that a second `Claim` returns the same URLs after the lease expires.

**T8.3 Final gate.** All of these must be true:
- `dotnet build SamoBot.sln -warnaserror` passes with no suppressions. Search for `pragma warning` and `SuppressMessage`. The result must be empty.
- All tests pass.
- `docker compose up` starts six services, and a job created through the API reaches `Completed`.
- A search for `RabbitMQ`, `Redis`, `Neo4j`, and `Samobot.` in `src/` returns no result.
