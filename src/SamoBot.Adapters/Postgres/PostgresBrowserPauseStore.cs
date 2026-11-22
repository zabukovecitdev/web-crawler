using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresBrowserPauseStore(NpgsqlDataSource dataSource) : IBrowserPauseStore
{
    private static readonly string PauseSql = $"""
        UPDATE "{TableNames.CrawlJobs}"
        SET "Status" = 'Paused', "PauseReason" = '{PauseReasons.BrowserUnavailable}', "PauseDetail" = NULL, "UpdatedAt" = @Now
        WHERE "Status" = 'Running' AND {PathRuleSql.UseBrowser}
        RETURNING "Id"
        """;

    private static readonly string ResumeSql = $"""
        UPDATE "{TableNames.CrawlJobs}"
        SET "Status" = 'Running', "PauseReason" = NULL, "PauseDetail" = NULL, "UpdatedAt" = @Now
        WHERE "Status" = 'Paused' AND "PauseReason" = '{PauseReasons.BrowserUnavailable}'
        RETURNING "Id"
        """;

    public Task<IReadOnlyList<int>> PauseJobsNeedingBrowser(DateTimeOffset now, CancellationToken ct) => Run(PauseSql, now, ct);

    public Task<IReadOnlyList<int>> ResumeJobsPausedForBrowser(DateTimeOffset now, CancellationToken ct) => Run(ResumeSql, now, ct);

    private async Task<IReadOnlyList<int>> Run(string sql, DateTimeOffset now, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var ids = await connection.QueryAsync<int>(new CommandDefinition(sql, new { Now = now }, cancellationToken: ct));
        return ids.ToList();
    }
}
