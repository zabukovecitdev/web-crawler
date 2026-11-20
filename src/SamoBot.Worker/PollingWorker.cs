using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using SamoBot.Core.Common;

namespace SamoBot.Worker;

public sealed partial class PollingWorker<THandler>(
    IServiceScopeFactory scopes,
    TimeProvider timeProvider,
    ILogger<PollingWorker<THandler>> logger) : BackgroundService
    where THandler : IWorkHandler
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var delay = await RunBatch(stoppingToken);
            if (delay > TimeSpan.Zero)
            {
                await WaitFor(delay, stoppingToken);
            }
        }
    }

    private async Task<TimeSpan> RunBatch(CancellationToken ct)
    {
        try
        {
            await using var scope = scopes.CreateAsyncScope();
            var processed = await scope.ServiceProvider.GetRequiredService<THandler>().HandleBatch(ct);
            return processed == 0 ? PollingDelays.Idle : TimeSpan.Zero;
        }
        catch (Exception ex) when (!ct.IsCancellationRequested)
        {
            LogBatchFailed(ex, typeof(THandler).Name);
            return PollingDelays.Error;
        }
        catch (OperationCanceledException)
        {
            return TimeSpan.Zero;
        }
    }

    private async Task WaitFor(TimeSpan delay, CancellationToken ct) =>
        await Task.Delay(delay, timeProvider, ct).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

    [LoggerMessage(EventId = 1, Level = LogLevel.Error, Message = "The batch of {Handler} failed. The worker retries after a delay.")]
    private partial void LogBatchFailed(Exception exception, string handler);
}
