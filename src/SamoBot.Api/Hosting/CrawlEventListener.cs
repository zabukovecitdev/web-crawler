using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.SignalR;
using Npgsql;
using SamoBot.Adapters.Postgres;
using SamoBot.Api.Hubs;

namespace SamoBot.Api.Hosting;

/// <summary>
/// Listens to the PostgreSQL channel of crawl events on one connection and sends each event
/// to the SignalR group of its job. A lost connection is opened again after a short wait.
/// </summary>
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
            // The host is stopping.
        }
    }

    private async Task WaitBeforeReconnect(CancellationToken ct)
    {
        try
        {
            await Task.Delay(ReconnectDelay, timeProvider, ct);
        }
        catch (OperationCanceledException)
        {
            // The host is stopping.
        }
    }

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
            // The host is stopping.
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Listening to crawl events.")]
    private partial void LogListening();

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "The crawl event connection was lost. It opens again after a delay.")]
    private partial void LogConnectionLost(Exception exception);

    [LoggerMessage(EventId = 3, Level = LogLevel.Warning, Message = "A crawl event message was not valid JSON.")]
    private partial void LogBadMessage(Exception exception);
}
