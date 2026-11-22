using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresReviewPauseStore(NpgsqlDataSource dataSource) : IReviewPauseStore
{
    private const string PauseSql = $"""
        UPDATE "{TableNames.CrawlJobs}" j
        SET "Status" = 'Paused', "PauseReason" = '{PauseReasons.Review}', "PauseDetail" = NULL, "UpdatedAt" = @Now
        WHERE j."Status" = 'Running' AND j."ReviewAfterPages" IS NOT NULL
          AND (SELECT count(*) FROM (
                   SELECT 1 FROM "{TableNames.DiscoveredUrls}" u
                   WHERE u."CrawlJobId" = j."Id" AND u."LastFetchId" IS NOT NULL
                   LIMIT j."ReviewAfterPages") fetched) >= j."ReviewAfterPages"
        RETURNING j."Id"
        """;

    public async Task<IReadOnlyList<int>> PauseJobsAtTheReviewPoint(DateTimeOffset now, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var ids = await connection.QueryAsync<int>(new CommandDefinition(PauseSql, new { Now = now }, cancellationToken: ct));
        return ids.ToList();
    }
}
