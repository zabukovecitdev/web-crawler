# 0013. Background loops and shutdown

Status: Accepted

## Decision

- `PollingWorker<THandler>` runs each `IWorkHandler` in a loop, and each batch gets its own DI scope. It waits
  `PollingDelays.Idle` after an empty batch and `PollingDelays.Error` after an exception.
- `JobCompletionHandler` returns 0 on purpose, so the polling loop waits between runs.
- Stopping the host cancels the waits. That cancellation is expected. The waits suppress it
  (`ConfigureAwaitOptions.SuppressThrowing`), so the loop ends at its next check. The event listener logs it at Debug level.
- `CrawlEventListener` listens to the PostgreSQL event channel on one connection and sends each event to the SignalR group of
  its job. When the connection is lost, the listener opens it again after a short wait. A message that is not valid JSON is
  logged and skipped.
- The MCP server sends all logs to stderr, because stdout carries the MCP protocol messages.
