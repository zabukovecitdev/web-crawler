# Simplify crawl links and recrawl plan

PR #34 (link states, recrawl) and PR #33 (Markdown and plain text, reprocess) work, but #34 has three problems:

- it spreads the state of one link over three layers,
- it erases a page's last fetch when the page is recrawled,
- with #33 it adds five ways to run work again.

This plan removes the duplication and fixes two bugs that come from it. The user-facing features stay: link states, reasons, filters, recrawl, retry failed and reprocess.

Each task is small, names its files and has a check. Agents can do the tasks (section 6).

## 0. How to use this plan

1. Read sections 1 to 5 before a task. Do not change a decision in section 3. Report a problem with it instead.
2. Do the tasks of a track in ID order. Track A (backend) and track B (frontend) can run in parallel (section 6.1).
3. After each task, run the checks in section 5. Do not go on when a check fails.
4. Change only the files the task names, plus their tests.
5. If the code does not match this plan, stop and report it. Do not guess.
6. Make one commit per task. Start the message with the task ID, for example `A1 Keep the last fetch when a link is requeued`.
7. Never skip, disable or weaken a test to get green. A test that asserts removed behaviour is changed or deleted *in the task that removes the behaviour*, and the commit message says so.

## 1. Findings this plan fixes

| # | Finding | Where |
|---|---|---|
| F1 | **A recrawl blanks the page screens.** `RequeueSql` sets `"LastFetchId" = NULL`. The page read model and the site statistics join on `LastFetchId`. Until a page is fetched again, its status, title, parsed content and metadata are gone, and the site's page count drops. | `PostgresUrlFrontier.cs` (`RequeueSql`), `PageSql.cs:14`, `SiteSql.cs:11` |
| F2 | **"Waiting to be fetched" has three definitions.** The completion check uses `LastCrawlAt IS NULL`. The progress counter and the link-state `CASE` use `LastFetchId IS NULL`. Reprocess (#33) clears only `LastCrawlAt`. F1 exists to satisfy the second definition. | `PostgresCrawlJobRepository.cs` (`CompleteSql`), `PostgresJobProgressReader.cs` (`ProgressSql`), `PostgresJobLinkReader.cs` (`StateExpression`), `PostgresUrlFrontier.cs` (`RefetchSkippedSql`) |
| F3 | **The steps of each link duplicate its state.** SQL derives `State`. C# maps every state to four step statuses (`LinkSteps`, `LinkStepStatus`, the `Looks` table, `WasRequested`). The frontend shows them as pips next to a state badge that says the same thing. | `JobLink.cs`, `LinkStatusDescriber.cs`, `LinkStepPips.tsx`, `link-format.ts` |
| F4 | **The Store step of a job is wrong.** `jobSteps` computes Store's status as `stepStatus(stored, stored, started)`, so it is "done" as soon as one page is stored. It counts against `fetched`, which includes failed and skipped links that can never be stored, so the bar never reaches 100 %. | `link-format.ts` (`jobSteps`) |
| F5 | **`JobLinkSummary` is a dictionary dressed as fields.** It has nine computed properties, a `[JsonIgnore]` on the dictionary, and a `Total` computed by summing. | `JobLink.cs` |
| F6 | **Five "run again" controls.** Recrawl all, Retry failed, per-link Recrawl, Recrawl this page, Reprocess, plus `NewCrawlForLinkDialog`, which exists only because one link of a cancelled job cannot be recrawled. Recrawl and Reprocess sit in different places with no hint of how they differ. | `RecrawlActions.tsx`, `CrawlActions.tsx` (`ReprocessAction`), `JobLinksPanel.tsx`, `NewCrawlForLinkDialog.tsx` |

## 2. Out of scope (owner decisions, do not do)

- `docs/content-processing-pipeline-report.md` (678 lines, an unbuilt design). #35 has since built the Markdown step differently. Keep it, trim it or delete it: **the owner decides; agents leave it alone.**
- The HTML source viewer with its own highlighter (`HtmlSource.tsx`, `HtmlSourceLines.tsx`, `html-highlight.ts`). It is large for what it does, but it works and nothing depends on it. Leave it.
- The state `CASE` stays in SQL. It is the one source the list, the filters and the counts share (that part of #34 is good).
- The reason sentences (`LinkReasons`) stay on the backend: they need `FailReason`, the status code and the page policy.
- No change to the API routes or job actions (`recrawl`, `retry-failed`, `reprocess`, `POST …/links/{linkId}/recrawl`).

## 3. Decisions

### D1. One rule for "waiting to be fetched": `LastCrawlAt`
A link waits for a fetch when `Status = 'Idle' AND ("LastCrawlAt" IS NULL OR "FailCount" > 0)`. This is already the rule of the completion check and of reprocess.

`LastFetchId` keeps pointing at the newest fetch for the whole life of the link, so the page screens never go blank. A requeue sets `"LastCrawlAt" = NULL` and leaves `"LastFetchId"` alone.

The rule lives in **one** C# helper that the four queries use:

```csharp
// src/SamoBot.Adapters/Postgres/FrontierRules.cs
internal static class FrontierRules
{
    /// <summary>SQL that is true when the DiscoveredUrls row aliased <paramref name="alias"/> waits for a fetch: new, requeued or retrying.</summary>
    public static string WaitingForFetch(string alias) =>
        $"""({alias}."Status" = 'Idle' AND ({alias}."LastCrawlAt" IS NULL OR {alias}."FailCount" > 0))""";
}
```

SQL strings that use it change from `const` to `static readonly`.

Rows from older requeues already have both columns NULL. That state is valid under the new rule, so no migration is needed.

### D2. A link's steps are not in the API
Remove `steps` from `JobLinkDto`, together with `LinkSteps`, `LinkStepStatus` and the step half of `LinkStatusDescriber`. The state badge and the reason sentence already say where a link is.

The job-level four steps stay: they are counts (`jobSteps`), used by the step bars and the progress ring.

### D3. `JobLinkSummary` is a plain record with the JSON unchanged
It becomes a record with ten `int` properties (`Total`, `Queued`, … `Failed`) and `Problems`. `PostgresJobLinkReader.ToSummary` builds it. The JSON the dashboard reads stays byte-for-byte the same.

### D4. One "Run again" menu
Replace `RecrawlActions` and `ReprocessAction` with one component, `RunAgainMenu`: a button labelled **Run again** that opens a `DropdownMenu` (`components/ui/dropdown-menu.tsx`). Its items:

| Item | Shown when | Action | Confirm |
|---|---|---|---|
| Retry {n} failed links | `links.failed + links.retrying > 0` | `retry-failed` | no |
| Fetch all links again | always (once the job has started) | `recrawl` | yes, the existing `ConfirmDialog` text |
| Re-read stored pages | `status === 'Completed'` | `reprocess` | no |

Each item has a one-line description under its label in muted text: what is fetched from the site and what is not. Errors show under the button, as today. It is used on the crawls list row and on the crawl detail page.

### D5. No per-link recrawl on a cancelled job; no dialog
On a cancelled job the per-link **Recrawl** button is disabled, with the title "This crawl was cancelled. Use Run again on the crawl, or request a new crawl." Delete `NewCrawlForLinkDialog`.

The backend rule in `CrawlJob.Reopen` stays: it already refuses with 409. **Recrawl this page** on the page screen stays. Its existing error message covers the cancelled case.

## 4. Target shape (after)

```
Backend
  PostgresJobLinkReader.StateExpression   ← Retrying, then Queued via LastCrawlAt (D1)
  FrontierRules.WaitingForFetch           ← used by CompleteSql, ProgressSql, StateExpression and the frontier
  RequeueSql                              ← no longer touches LastFetchId
  JobLink                                 ← no Steps
  LinkStatusDescriber                     ← LinkState → reason only (a Dictionary<LinkState, Func<JobLinkFacts,string>>)
  JobLinkSummary                          ← plain record, same JSON
Frontend
  JobLinksTable                           ← columns: State, Link, Why, Recrawl (no Steps column)
  link-format.ts                          ← no stepLook / STEP_NAMES / StepLook; jobSteps Store fixed
  RunAgainMenu.tsx                        ← replaces RecrawlActions + ReprocessAction
  deleted: LinkStepPips.tsx, NewCrawlForLinkDialog.tsx, RecrawlActions.tsx
```

## 5. Check commands

Setup in each shell, for an agent in a cloud container:

```bash
export PATH=$HOME/.dotnet:$PATH DOTNET_CLI_TELEMETRY_OPTOUT=1
```

| When | Command |
|---|---|
| After each A task | `dotnet build SamoBot.sln -warnaserror` and `dotnet test src/SamoBot.Tests --filter "Category!=Integration"` |
| After each A task that changes SQL (A1, A3) | `dotnet test src/SamoBot.Tests --filter "Category=Integration"` (needs Docker) |
| After each B task, in `src/frontend` | `npm run lint && npm run typecheck && npm test && npm run build` |

## 6. Tasks

### 6.1 Order

```
Track A (backend):   A1 ── A2 ── A3
Track B (frontend):  B1 ── B2 ── B3        B1 needs A2's contract (section 3, D2); it can start at once,
                                            because the frontend only stops reading a field.
C1 after both tracks.
```

### Track A: backend

**A1. Keep the last fetch on requeue; one "waiting" rule (F1, F2, D1).**
- Add `src/SamoBot.Adapters/Postgres/FrontierRules.cs` exactly as in D1.
- `PostgresCrawlJobRepository.CompleteSql`: replace the inline `(u."Status" = 'Idle' AND (u."LastCrawlAt" IS NULL OR u."FailCount" > 0))` with `{FrontierRules.WaitingForFetch("u")}`.
- `PostgresJobProgressReader.ProgressSql`, `"Queued"`: `count(*) FILTER (WHERE {FrontierRules.WaitingForFetch("d")})`.
- `PostgresJobLinkReader.StateExpression`: keep the order. Replace the line `WHEN u."Status" = 'Idle' AND u."LastFetchId" IS NULL THEN 'Queued'` with `WHEN u."Status" = 'Idle' AND u."LastCrawlAt" IS NULL THEN 'Queued'`. It comes right after the `Retrying` line, which already catches `FailCount > 0`. Keep `WHEN f."Id" IS NULL THEN 'Queued'` as the fallback.
- `PostgresUrlFrontier.RequeueSql`: remove `"LastFetchId" = NULL`. Keep `"LastCrawlAt" = NULL`. Update the comment above it: a requeued link waits because `LastCrawlAt` is null, and its last fetch stays so the page screens keep showing it.
- Tests (integration, in the existing Postgres test classes that cover these readers; find them with `grep -rln "Requeue\|ProgressSql\|PostgresJobLinkReader" src/SamoBot.Tests`):
  1. After a fetch, `Requeue(job, RequeueScope.All)`: `IPageReader.FindPage(id)` still returns the status code, title and `Parsed`; `ISiteReader` still counts the page.
  2. The requeued link's state is `Queued`; the job progress counts it as queued; `CompleteSql` does not complete the job while it waits.
  3. After the link is fetched again, `LastFetchId` points at the new fetch and the state moves on as before.
- Check: unit and integration tests.

**A2. Drop a link's steps from the API (F3, D2).**
- `JobLink.cs`: delete `LinkStepStatus`, `LinkSteps` and `JobLink.Steps`.
- `LinkStatusDescriber.cs`: the table becomes `Dictionary<LinkState, Func<JobLinkFacts, string>>` (reasons only). Delete `Look`, `StoppedAtFetch`, `NeverRequested` and `WasRequested`. `Describe` keeps every other field.
- `LinkStatusDescriberTests.cs`: delete the assertions on steps. Keep and, where missing, add one reason assertion per state. The commit message lists the deleted step assertions.
- `docs/api-contract.md`, the `GET /api/v1/crawl-jobs/{id}/links` row: remove "and the status of its steps (`fetch`, `store`, `parse`, `index`)".
- `ApiJsonContractTests`: if it lists `JobLinkDto` properties, remove `steps` there.
- Check: unit tests.

**A3. `JobLinkSummary` as a plain record (F5, D3).**
- First, add a test to `ApiJsonContractTests` (or the existing contract test for `CrawlJobDto`): it serialises a `CrawlJobDto` with a known summary and asserts the exact JSON of `links`, meaning the property names and values of `total` … `failed` and `problems`. **Commit this test first, inside A3, before changing the type.** It must pass on the old type.
- `JobLink.cs`: `public sealed record JobLinkSummary(int Total, int Queued, int Fetching, int Retrying, int Parsing, int Indexing, int Indexed, int NotIndexed, int Skipped, int Failed, IReadOnlyList<LinkProblem> Problems)` with `public static readonly JobLinkSummary Empty`. Remove the dictionary, `Of`, and `[JsonIgnore]`.
- `PostgresJobLinkReader.ToSummary`: build the record from the grouped counts; `Total` is their sum.
- Fix the other users. Find them with `grep -rn "JobLinkSummary\|\.Counts" src --include=*.cs`.
- Check: the contract test passes unchanged; unit and integration tests pass.

### Track B: frontend (`src/frontend/src/features/crawl/`)

**B1. Remove the per-link step pips (F3, D2).**
- Delete `LinkStepPips.tsx`.
- `JobLinksTable.tsx`: remove the Steps column (header and cell). Columns: State, Link, Why, actions.
- `link-format.ts`: delete `StepLook`, `STEP_LOOKS`, `stepLook` and `STEP_NAMES`. Check first with `grep -rn "stepLook\|STEP_NAMES" src`; nothing else may use them.
- `src/types/api.ts`: remove `LinkStepStatus`, `LinkSteps` and `JobLinkDto.steps`.
- `src/mocks/`: remove `steps` from mock links.
- Check: frontend checks.

**B2. Fix the Store step (F4).**
- `link-format.ts` `jobSteps`: Store counts against the links that could be stored, `storable = fetched - links.failed - links.skipped`. Its status is `stepStatus(stored, storable, started)`. Leave the other three steps unchanged.
- `link-format.test.ts`: add cases for `jobSteps`:
  - mid-crawl, stored < storable → Store `active`;
  - all storable stored and some skipped → Store `done` at 100 %;
  - nothing fetched → Store `waiting`.
- Check: frontend checks.

**B3. One Run again menu; no cancelled-link dialog (F6, D4, D5).**
- Add `RunAgainMenu.tsx` as in D4. Reuse the existing mutation hooks: `useRecrawl` from `RecrawlActions.tsx` and `useJobAction` from `CrawlActions.tsx`; move them if needed. Reuse `ConfirmDialog` for "Fetch all links again". Return `null` for Draft or Pending jobs and for jobs with no links, as `RecrawlActions` does now.
- `CrawlsTable.tsx` and `CrawlDetailPage.tsx`: use `RunAgainMenu` instead of `RecrawlActions` and `ReprocessAction`.
- Delete `RecrawlActions.tsx` (after moving what you reuse) and `ReprocessAction` from `CrawlActions.tsx`.
- `JobLinksPanel.tsx`: remove `NewCrawlForLinkDialog` and the `asNewCrawl` state. Pass `disabledReason` to `JobLinksTable` when `job.status === 'Cancelled'`. `JobLinksTable` disables Recrawl and sets the D5 title when the reason is set. Delete `NewCrawlForLinkDialog.tsx`.
- Add `RunAgainMenu`'s visibility rules as a pure function, `runAgainItems(job)` in `run-again.ts`, with a vitest table test over the job states (Running, Paused, Completed, Cancelled, Failed) and failed counts.
- Check:
  - frontend checks;
  - `npm run dev` with mocks (`VITE_USE_MOCKS=true`), Playwright screenshots at deviceScaleFactor 1 of the crawls list (menu open) and of a crawl detail page.

Save the screenshots to the session scratchpad. Do not commit them.

### C1. Finish

- `README.md`: if it mentions Recrawl, Retry failed or Reprocess, describe the one **Run again** menu instead.
- Write a short note for the PR description: the before and after for F1 to F6, and the lines removed (`git diff --stat origin/main`).
- Manual check with `docker compose up`:
  1. Crawl a small site and recrawl it. The page screens keep their content while the links are queued.
  2. Retry failed works.
  3. Re-read stored pages works on a completed crawl.
  4. On a cancelled crawl, the per-link Recrawl button is disabled with the explanation.

## 7. Expected result

- **Bugs fixed:** F1 (blank pages after a recrawl) and F4 (Store step).
- **Removed:** about 300–400 lines across the backend and frontend; three frontend files deleted (`LinkStepPips.tsx`, `NewCrawlForLinkDialog.tsx`, `RecrawlActions.tsx`); two C# types deleted (`LinkSteps`, `LinkStepStatus`).
- **One definition** of "waiting to be fetched", one place to read job "run again" actions, and the same features for the user.
