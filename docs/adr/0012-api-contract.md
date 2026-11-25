# 0012. API contract

Status: Accepted

## Context

`docs/api-contract.md` describes the HTTP API. `src/frontend/src/types/api.ts` holds the same shapes in TypeScript.

## Decision

- The JSON shapes are fixed. `ApiJsonContractTests` fails when one changes. These C# records mirror TypeScript types by
  property name: `CrawlJobDto`, `CrawlProgress`, `SitemapDto`, `JobEvent`, `JobPage`. Change both sides together.
- The dashboard reads the string values of `ContentStatus`, `CrawlEventTypes` and `LinkState` as they are. Renaming one is a
  breaking change.
- A crawl event is flat JSON, `{ crawlJobId, eventType, at, ...fields }`, plus `id` once stored (`CrawlEventEnvelope`). The
  SignalR push and the events endpoint use this one shape. The reserved names win over a payload field of the same name. The
  hub is `/hubs/crawl-job`, the methods are `JoinJob(jobId)` and `LeaveJob(jobId)`, the group of a job is `crawlJob:{id}` and the
  client event is `CrawlEvent`.
- Without `after`, the events endpoint returns the newest events, oldest first.
- The page id of the dashboard is the discovered URL id. A site is a host: its row aggregates every job that touched the host.
- Host keys are `Uri.Authority` values, so they round-trip in the `{host}` route value.
- A refused job action returns 404 for an unknown job, else 409 with the reason.
- A host without a stored sitemap gives an empty result, not 404.
- The list endpoints have caps (`ListLimits`), so a request cannot make the database scan far past the useful range.
- `X-Content-Stored-Bytes` gives the size of the whole stored body, so a reader can check that it has all of it.
- Analytics: a missing bucket is "hour" for the 24 h range and "day" otherwise. A timeseries returns at most 1000 buckets, and an
  older start is cut so the chart stays small.
