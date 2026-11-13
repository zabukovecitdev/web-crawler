using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>Stores the robots.txt rules of each host. A crawl delay in the rules also sets the delay of the host.</summary>
public sealed class PostgresRobotsStore(NpgsqlDataSource dataSource) : IRobotsStore
{
    private const int MaxCrawlDelayMs = 60_000;

    private const string GetSql = $"""
        SELECT "Host", "Content", "FetchedAt", "ExpiresAt" FROM "{TableNames.RobotsTxt}" WHERE "Host" = @Host
        """;

    private const string SaveSql = $"""
        INSERT INTO "{TableNames.RobotsTxt}" ("Host", "Content", "ParsedRules", "FetchedAt", "ExpiresAt", "CrawlDelayMs")
        VALUES (@Host, @Content, '[]'::jsonb, @FetchedAt, @ExpiresAt, @CrawlDelayMs)
        ON CONFLICT ("Host") DO UPDATE SET
            "Content" = EXCLUDED."Content", "FetchedAt" = EXCLUDED."FetchedAt",
            "ExpiresAt" = EXCLUDED."ExpiresAt", "CrawlDelayMs" = EXCLUDED."CrawlDelayMs"
        """;

    private const string SetHostDelaySql = $"""
        UPDATE "{TableNames.Hosts}" SET "CrawlDelayMs" = @CrawlDelayMs WHERE "Host" = @Host
        """;

    public async Task<RobotsRules?> Find(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var row = await connection.QuerySingleOrDefaultAsync<StoredRules>(new CommandDefinition(GetSql, new { Host = host }, cancellationToken: ct));
        return row is null ? null : new RobotsRules(row.Host, row.Content, row.FetchedAt, row.ExpiresAt);
    }

    public async Task Save(RobotsRules rules, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var parameters = new
        {
            rules.Host,
            rules.Content,
            rules.FetchedAt,
            rules.ExpiresAt,
            CrawlDelayMs = rules.CrawlDelay is { } delay ? (int?)Math.Min((int)delay.TotalMilliseconds, MaxCrawlDelayMs) : null
        };

        await connection.ExecuteAsync(new CommandDefinition(SaveSql, parameters, transaction, cancellationToken: ct));
        if (parameters.CrawlDelayMs is not null)
        {
            await connection.ExecuteAsync(new CommandDefinition(SetHostDelaySql, parameters, transaction, cancellationToken: ct));
        }

        await transaction.CommitAsync(ct);
    }
}

/// <summary>The row of a stored robots.txt. Dapper fills the properties.</summary>
internal sealed record StoredRules
{
    public string Host { get; init; } = string.Empty;
    public string Content { get; init; } = string.Empty;
    public DateTimeOffset FetchedAt { get; init; }
    public DateTimeOffset ExpiresAt { get; init; }
}
