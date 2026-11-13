using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>Read-only queries for the dashboard.</summary>
public sealed class PostgresJobPageReader(NpgsqlDataSource dataSource) : IJobPageReader
{
    private const string ListPagesSql = $"""
        SELECT "Id", "Host", "Url", "NormalizedUrl", "Status", "LastCrawlAt", "NextCrawlAt", "FailCount", "FailReason",
               "DiscoveredAt", "Priority", "LastFetchId", "StatusUpdatedAt", "CrawlJobId", "Depth", "UseJsRendering",
               "RespectRobots"
        FROM "{TableNames.DiscoveredUrls}"
        WHERE "CrawlJobId" = @CrawlJobId
        ORDER BY "Id"
        LIMIT @Limit OFFSET @Offset
        """;

    private const string CountPagesSql = $"""SELECT count(*) FROM "{TableNames.DiscoveredUrls}" WHERE "CrawlJobId" = @CrawlJobId""";

    private const string ListEventsSql = $"""
        SELECT "Id", "CrawlJobId", "EventType", "Payload", "CreatedAt"
        FROM "{TableNames.CrawlJobEvents}"
        WHERE "CrawlJobId" = @CrawlJobId AND "Id" > @AfterId
        ORDER BY "Id"
        LIMIT @Limit
        """;

    public Task<IReadOnlyList<JobPage>> ListPages(int crawlJobId, int limit, int offset, CancellationToken ct) =>
        Query<JobPage>(ListPagesSql, new { CrawlJobId = crawlJobId, Limit = limit, Offset = offset }, ct);

    public async Task<int> CountPages(int crawlJobId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await connection.ExecuteScalarAsync<int>(new CommandDefinition(CountPagesSql, new { CrawlJobId = crawlJobId }, cancellationToken: ct));
    }

    public Task<IReadOnlyList<JobEvent>> ListEvents(int crawlJobId, long afterId, int limit, CancellationToken ct) =>
        Query<JobEvent>(ListEventsSql, new { CrawlJobId = crawlJobId, AfterId = afterId, Limit = limit }, ct);

    private async Task<IReadOnlyList<T>> Query<T>(string sql, object parameters, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var rows = await connection.QueryAsync<T>(new CommandDefinition(sql, parameters, cancellationToken: ct));
        return rows.ToList();
    }
}
