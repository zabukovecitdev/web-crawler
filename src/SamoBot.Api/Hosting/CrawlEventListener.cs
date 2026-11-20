using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR;
using Npgsql;
using SamoBot.Adapters.Postgres;
using SamoBot.Api.Hubs;

namespace SamoBot.Api.Hosting;

public sealed partial class CrawlEventListener(
    NpgsqlDataSource dataSource,
    IHubContext<CrawlJobHub> hub,
    TimeProvider timeProvider,
    ILogger<CrawlEventListener> logger) : BackgroundService
{
    private static readonly TimeSpan ReconnectDelay = TimeSpan.FromSeconds(5);

    private readonly Channel<string> _messages = Channel.CreateUnbounded<string>();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var forwarding = Forward(stoppingToken);
        while (!stoppingToken.IsCancellationRequested)
        {
            await ListenUntilLost(stoppingToken);
        }

        _messages.Writer.Complete();
        await forwarding;
    }

    private async Task ListenUntilLost(CancellationToken ct)
    {
        try
        {
            await using var connection = await dataSource.OpenConnectionAsync(ct);
            connection.Notification += (_, args) => _messages.Writer.TryWrite(args.Payload);
            await using (var listen = new NpgsqlCommand($"LISTEN {PostgresCrawlEventPublisher.Channel}", connection))
            {
                await listen.ExecuteNonQueryAsync(ct);
            }

            LogListening();
            while (true)
            {
                await connection.WaitAsync(ct);
            }
        }
        catch (Exception ex) when (ex is NpgsqlException or IOException)
        {
            LogConnectionLost(ex);
            await WaitBeforeReconnect(ct);
        }
        catch (OperationCanceledException)
        {
            LogStopped();
        }
    }

    private async Task WaitBeforeReconnect(CancellationToken ct) =>
        await Task.Delay(ReconnectDelay, timeProvider, ct).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

    private async Task Forward(CancellationToken ct)
    {
        await foreach (var message in _messages.Reader.ReadAllAsync(CancellationToken.None))
        {
            await SendToGroup(message, ct);
        }
    }

    private async Task SendToGroup(string message, CancellationToken ct)
    {
        try
        {
            using var document = JsonDocument.Parse(message);
            var root = document.RootElement;
            if (root.TryGetProperty("crawlJobId", out var id) && id.TryGetInt32(out var jobId))
            {
                await hub.Clients.Group(CrawlJobHub.JobGroupName(jobId)).SendAsync("CrawlEvent", root, ct);
            }
        }
        catch (JsonException ex)
        {
            LogBadMessage(ex);
        }
        catch (OperationCanceledException)
        {
            LogEventDropped();
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Listening to crawl events.")]
    private partial void LogListening();

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "The crawl event connection was lost. It opens again after a delay.")]
    private partial void LogConnectionLost(Exception exception);

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning, Message = "A crawl event message was not valid JSON.")]
    private partial void LogBadMessage(Exception exception);

    [LoggerMessage(EventId = 4, Level = LogLevel.Debug, Message = "Stopped listening to crawl events: the host is stopping.")]
    private partial void LogStopped();

    [LoggerMessage(EventId = 5, Level = LogLevel.Debug, Message = "A crawl event was not sent: the host is stopping.")]
    private partial void LogEventDropped();
}
