using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresJobTreeReader(NpgsqlDataSource dataSource) : IJobTreeReader
{
    private static readonly string FoldersSql = FolderGroupSql.Group(
        $"""
        SELECT {PathRuleSql.PathOf("u.\"NormalizedUrl\"")} AS path,
               CASE
                   WHEN u."Status" = 'InFlight' THEN 'fetching'
                   WHEN u."Status" = 'Disabled' AND u."FailReason" = '{UrlFailReasons.Unselected}' THEN 'stopped'
                   WHEN u."Status" = 'Disabled' AND u."FailReason" IN ('redirect', 'robots', 'blocked-address') THEN 'skipped'
                   WHEN u."Status" = 'Disabled' AND u."FailReason" IS NOT NULL THEN 'failed'
                   WHEN u."Status" = 'Idle' AND (u."LastFetchId" IS NULL OR u."FailCount" > 0) THEN 'queued'
                   ELSE 'done'
               END AS state
        FROM "{TableNames.DiscoveredUrls}" u WHERE u."CrawlJobId" = @JobId
        """,
        """
        count(*)::int AS "Pages",
        count(*) FILTER (WHERE state = 'done')::int AS "Done",
        count(*) FILTER (WHERE state = 'failed')::int AS "Failed",
        count(*) FILTER (WHERE state = 'skipped')::int AS "Skipped",
        count(*) FILTER (WHERE state = 'stopped')::int AS "Stopped",
        count(*) FILTER (WHERE state = 'fetching')::int AS "Fetching",
        count(*) FILTER (WHERE state = 'queued')::int AS "Queued",
        NULL::timestamptz AS "OldestModified", NULL::timestamptz AS "NewestModified"
        """);

    public async Task<IReadOnlyList<FolderGroup>> Folders(int crawlJobId, string parent, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var rows = await connection.QueryAsync<FolderRow>(new CommandDefinition(FoldersSql, new { JobId = crawlJobId, Parent = parent }, cancellationToken: ct));
        return rows.Select(row => row.ToGroup()).ToList();
    }
}
