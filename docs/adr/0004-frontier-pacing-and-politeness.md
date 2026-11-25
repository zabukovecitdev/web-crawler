# 0004. Frontier, in-flight hold and politeness

Status: Accepted

## Context

Several workers claim URLs from one PostgreSQL frontier. Each host must see at most one request at a time and a polite
delay between requests. The sections "The claim pattern" and "Politeness (adaptive delay)" of [architecture.md](../architecture.md) give an overview.

## Decision

**Claim (`PostgresUrlFrontier`, `IUrlFrontier.Claim`).**
- A claim takes at most one due URL for each host and locks the host row, so two workers never claim the same host at once.
  All times come from `TimeProvider`.
- Each claimed host is held for the in-flight hold, so no second request goes to it while this one runs. The hold must not
  be longer than the lease.

**In-flight hold (`InFlightHold`, `CrawlBatchHandler.Lease`).**
- The hold is the longest possible fetch (the larger of the request deadline and the browser total deadline) plus
  `Crawler:InFlightMarginSeconds` (default 5). A claim puts `NextAllowedAt` this far ahead, and completing the URL releases it.
- If a worker dies, the hold simply expires.
- A deadline of zero or less means the fetch has no deadline, so the hold is the cap, which is the URL lease (5 minutes). The
  hold never outlives the lease.
- `ClaimedUrl.HeldUntil` is set by the claim. When it is null, no hold is known, so completing keeps the later wait.

**Release (`PostgresUrlFrontier.ReleasedWait`, `ReleaseHost`, `FetchRecorder`).**
- Every outcome releases the hold in the same transaction as the result (`IUrlFrontier.Complete`). The fetch row, the new
  URL state and the pacing of the host are stored together.
- The new wait is completion time plus the pacing delay. It *replaces* the hold. `GREATEST` with the hold would keep the host
  closed for the whole hold.
- A stored value within the hold (at most `@HeldUntil`, plus 1 ms for rounding) is this claim's own hold or an older wait,
  and it is replaced. A later value is a wait that someone booked on purpose while the request ran (a sitemap fetch queued
  behind it, or the hold of a second claim after this one expired). That wait stays, so the release never moves a host
  backwards past a wait it must keep. Without a known hold, the later value always stays.
- The host row is updated before the URL row, in the same order as the claim locks them, so the two never wait for each other.
- An outcome with no pacing change (a blocked address, a robots disallow) still releases the hold, with the base delay. A
  robots disallow made no request, so the host keeps its pacing state.

**URL outcomes (`OutcomeClassifier`, `CrawlOutcome`, `BackoffPolicy`).**
- `OutcomeClassifier` is the only place that maps a fetch result to an outcome. Each `CrawlOutcome` type builds its own
  `UrlUpdate`, so `IUrlFrontier.Complete` writes the update without a branch on the outcome type.
- The URL retry rules are separate from the host pacing below. Pacing only decides when the host is claimed again.

| Fetch result | Outcome | URL state afterwards |
|---|---|---|
| robots.txt disallows the URL (no request is made, `FetchRecorder`) | `Disabled("robots")` | Disabled, reason `robots` |
| every address of the host is denied | `Blocked` | Disabled, reason `blocked-address` |
| 200 to 299 | `Succeeded` | Idle, due again after `RecrawlInterval` (1 day), failure count 0 |
| 301, 302, 303, 307 or 308 with a `Location` | `Redirected` | Disabled, reason `redirect`; an http or https target is queued at the same depth |
| 0 (network error or timeout), 429, 500 to 599 | `RetryLater` | Idle, failure count + 1, due after `BackoffPolicy.DelayFor(failCount)`: 1 minute x 2^n, at most 1 hour |
| the same, when it is failure number `MaxFailures` (5) in a row | `Disabled("max-failures")` | Disabled, reason `max-failures` |
| any other status | `Disabled("http-<status>")` | Disabled, reason `http-<status>` |

**Politeness (`PolitenessPolicy`).** The policy is pure: numbers come in, and the new state and the delay go out.
- The base delay (robots.txt `Crawl-delay` or the default) is always the minimum. A crawl delay in the stored robots rules
  also sets the delay of the host (`PostgresRobotsStore`).
- A host that answered (status 200 to 499, except 429) updates the moving average of its response time (alpha 0.3) and
  resets the failure count. The delay is the larger of the base delay and 5 times the average.
- A timeout, a network error, a 5xx and a 429 count as failures. Each consecutive failure doubles the delay (2^n, n at most 10).
- The adaptive part (slow host, failures) is capped by `Crawler:MaxAdaptiveDelaySeconds` (default 60). The robots.txt
  delay is always the minimum, and a Retry-After is not subject to this cap.
- A 429 or 503 with Retry-After waits at least that long (at most 1 hour). Without Retry-After, the delay backs off from
  the base delay, 2^n, at most 10 minutes.
- A blocked address changes nothing (the policy returns null), and the host row keeps its state.
- `HostPacingUpdate.Delay` is never below the base delay of the host.

**Progress display (`CrawlProgress`, `JobProgressData`).**
- The adaptive delay wins the label when it is larger than the base delay, because it is the delay in effect.
- Only a running job has a next request time, and only while the host is not yet due.
- The delay shown is that of the host of the current URL. With no URL in flight, it is the host of the first seed.

## Consequences

Any write to `Hosts.NextAllowedAt` must respect the release rule above, or hosts get hammered or stalled.
