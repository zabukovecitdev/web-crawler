using System.Text.Json;
using Dapper;
using Npgsql;
using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// Stores a job event and tells the listeners with <c>pg_notify</c>.
/// The notification has the same envelope that the dashboard already reads.
/// </summary>
public sealed class PostgresCrawlEventPublisher(NpgsqlDataSource dataSource) : ICrawlEventPublisher
{
    public const string Channel = "crawl_events";

    private const string InsertSql = $"""
        INSERT INTO "{TableNames.CrawlJobEvents}" ("CrawlJobId", "EventType", "Payload")
        VALUES (@CrawlJobId, @EventType, @Payload)
        RETURNING "Id"
        """;

    private const string NotifySql = "SELECT pg_notify(@Channel, @Envelope)";

    public async Task Publish(int crawlJobId, string eventType, object payload, CancellationToken ct)
    {
        var payloadJson = JsonSerializer.Serialize(payload, JsonDefaults.Options);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var eventId = await connection.ExecuteScalarAsync<long>(
            new CommandDefinition(InsertSql, new { CrawlJobId = crawlJobId, EventType = eventType, Payload = payloadJson }, cancellationToken: ct));

        var envelope = JsonSerializer.Serialize(
            new { crawlJobId, eventId, eventType, payload = JsonSerializer.Deserialize<JsonElement>(payloadJson) },
            JsonDefaults.Options);
        await connection.ExecuteAsync(new CommandDefinition(NotifySql, new { Channel, Envelope = envelope }, cancellationToken: ct));
    }
}
