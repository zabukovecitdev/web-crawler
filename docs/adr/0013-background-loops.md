# 0013. Background loops and shutdown

Status: Accepted

## Decision

- `PollingWorker<THandler>` runs each `IWorkHandler` in a loop, and each batch gets its own DI scope. It waits
  `PollingDelays.Idle` (1 s) after an empty batch and `PollingDelays.Error` (5 s) after an exception.
- The worker runs five loops (`SamoBot.Worker/Program.cs`): crawl, parse, index, job completion and sitemaps.
- Batch sizes are constants of the handlers. `CrawlBatchHandler` claims 20 URLs with a 5-minute lease (`CrawlBatchHandler.Lease`).
  `ParseBatchHandler` claims 10 stored fetches with a 5-minute lease. `IndexBatchHandler` takes 50 pages without a lease.
- A parse batch runs its pages with `Task.WhenAll`, and the parse queue has no attempt limit. A fetch that always throws fails
  its batch and is claimed again when the lease ends.
- `JobCompletionHandler` returns 0 on purpose, so the polling loop waits between runs.
- Stopping the host cancels the waits. That cancellation is expected. The waits suppress it
  (`ConfigureAwaitOptions.SuppressThrowing`), so the loop ends at its next check. The event listener logs it at Debug level.
- `CrawlEventListener` listens to the PostgreSQL event channel on one connection and sends each event to the SignalR group of
  its job. When the connection is lost, the listener opens it again after a short wait. A message that is not valid JSON is
  logged and skipped.
- The MCP server sends all logs to stderr, because stdout carries the MCP protocol messages.
