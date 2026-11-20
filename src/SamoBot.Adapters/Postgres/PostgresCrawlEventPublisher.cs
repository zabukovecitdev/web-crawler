using System.Text;
using System.Text.Json;
using Dapper;
using Npgsql;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresCrawlEventPublisher(NpgsqlDataSource dataSource) : ICrawlEventPublisher
{
    public const string Channel = "crawl_events";

    private const int MaxNotifyBytes = 7900;

    private const string InsertSql = $"""
        INSERT INTO "{TableNames.CrawlJobEvents}" ("CrawlJobId", "EventType", "Payload")
        VALUES (@CrawlJobId, @EventType, @Payload)
        RETURNING "Id", "CreatedAt"
        """;

    private const string NotifySql = "SELECT pg_notify(@Channel, @Envelope)";

    public async Task Publish(int crawlJobId, string eventType, object payload, CancellationToken ct)
    {
        var payloadJson = JsonSerializer.Serialize(payload, JsonDefaults.Options);

        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var stored = await connection.QuerySingleAsync<(long Id, DateTimeOffset CreatedAt)>(
            new CommandDefinition(InsertSql, new { CrawlJobId = crawlJobId, EventType = eventType, Payload = payloadJson }, cancellationToken: ct));

        var envelope = CrawlEventEnvelope.Build(stored.Id, crawlJobId, eventType, stored.CreatedAt, payloadJson).ToJsonString();
        if (Encoding.UTF8.GetByteCount(envelope) <= MaxNotifyBytes)
        {
            await connection.ExecuteAsync(new CommandDefinition(NotifySql, new { Channel, Envelope = envelope }, cancellationToken: ct));
        }
    }
}
