using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresSitemapPlanReader(NpgsqlDataSource dataSource) : ISitemapPlanReader
{
    private const string StatusSql = $"""
        SELECT
            (SELECT count(*) FROM "{TableNames.SitemapFiles}" WHERE "Host" = @Host)::int AS "Files",
            (SELECT count(*) FROM "{TableNames.SitemapUrls}" WHERE "Host" = @Host)::int AS "Urls",
            r."RequestedAt", r."CompletedAt", COALESCE(r."Attempts", 0)::int AS "Attempts"
        FROM (SELECT 1) one
        LEFT JOIN "{TableNames.SitemapRequests}" r ON r."Host" = @Host
        """;

    public async Task<SitemapPlanStatus> Status(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var row = await connection.QuerySingleAsync<SitemapPlanStatusRow>(new CommandDefinition(StatusSql, new { Host = host }, cancellationToken: ct));
        return new SitemapPlanStatus(row.Files, row.Urls, row.RequestedAt, row.CompletedAt, row.Attempts);
    }
}

internal sealed record SitemapPlanStatusRow
{
    public int Files { get; init; }
    public int Urls { get; init; }
    public DateTimeOffset? RequestedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }
    public int Attempts { get; init; }
}
