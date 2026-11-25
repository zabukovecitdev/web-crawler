# 0003. Timeouts and deadlines

Status: Accepted

## Context

A slow or hostile server can stall a request at every step: DNS, connect, headers or a body sent one byte at a time. The
README section "Timeouts" lists the settings. These are the decisions behind them.

## Decision

- **No `HttpClient.Timeout`.** Clients added with `AddSafeHttpClient` set `Timeout.InfiniteTimeSpan`. Every request runs
  under a `RequestDeadline`, which also covers the body read. `HttpClient.Timeout` does not cover the body read.
- **`RequestDeadline`.** One total deadline covers everything, the body read included. A shorter first-byte deadline covers
  the time until the response headers arrive, and `HeadersReceived()` switches it off. A cancellation of the caller's own
  token also cancels `Token`, so the caller tells a timeout from a shutdown with its own token: `!ct.IsCancellationRequested`
  means a timeout. A duration of zero or less turns that deadline off. Each deadline is a `CancellationTokenSource` on the
  `TimeProvider`. That source handles a timer that fires while the request is being disposed, so tests can drive both deadlines with a fake clock.
- **Connect.** DNS plus connect (the cache refresh included) must finish within `Crawler:ConnectTimeoutSeconds`, or
  `SafeConnect` throws `TimeoutException`. TLS runs after the connect callback and falls under the first-byte deadline.
- **Status code 0 instead of exceptions.** `IFetcher` never returns null and never throws for an HTTP error status.
  `FetchResponse.StatusCode` 0 means a network error or a timeout, with the reason in `Error`. The fetchers do not throw
  for these. A cancellation by the caller (shutdown) is not a timeout and still throws. The browser fetcher also gives 0
  for a render error or a timeout (navigation, or the total cap).
- **Robots.txt (`HttpRobotsSource`).** The host key can include a non-default port, so the source tries https first and
  then http, each under its own deadline (`Crawler:RobotsTimeoutSeconds`). A found file gives its rules. A missing file (4xx,
  or a redirect that never ends) allows everything. A server error, a network error or a timeout blocks the host for a
  short time. A host or redirect hop with only disallowed addresses throws `BlockedAddressException`. A found or missing
  file is kept for 24 hours, a block for 1 hour. The body is read up to 512 KB.

| Setting (`Crawler:`) | Default | Covers | 0 |
|---|---|---|---|
| `ConnectTimeoutSeconds` | 5 | DNS and connect together | off |
| `FirstByteTimeoutSeconds` | 10 | from sending the request until the headers arrive | off |
| `RequestTimeoutSeconds` | 20 | a whole page request, body included | off |
| `RobotsTimeoutSeconds` | 10 | each robots.txt attempt (https, then http), body included | |
| `SitemapTimeoutSeconds` | 30 | each sitemap file, body included | |

| Setting (`ChromeRendering:`) | Default | Covers |
|---|---|---|
| `DefaultTimeoutMs` | 30000 | the navigation of a page, and the default of every other browser call |
| `TotalTimeoutMs` | 45000 | one cap around the whole render: connect, navigation and reading the page |

## Consequences

A new fetch path must create a `RequestDeadline` and must treat a timeout as status 0, not as an exception.
