using System.Text.Json;
using Dapper;
using Npgsql;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresCrawlJobRepository(NpgsqlDataSource dataSource) : ICrawlJobRepository
{
    private const string Columns = """
        "Id", "OwnerUserId", "SeedUrls", "AllowedHosts", "MaxDepth", "MaxUrls", "RenderMode", "JsNeeded",
        "SeedFromSitemap", "FollowLinkedPages", "PathRules", "ReviewAfterPages", "PauseReason", "PauseDetail",
        "CreatedAt", "Status", "UpdatedAt", "StartedAt", "CompletedAt"
        """;

    private const string GetSql = $"""SELECT {Columns} FROM "{TableNames.CrawlJobs}" WHERE "Id" = @Id""";

    private const string ListSql = $"""SELECT {Columns} FROM "{TableNames.CrawlJobs}" ORDER BY "Id" DESC LIMIT @Limit""";

    private const string InsertSql = $"""
        INSERT INTO "{TableNames.CrawlJobs}"
            ("OwnerUserId", "Status", "SeedUrls", "AllowedHosts", "MaxDepth", "MaxUrls", "RenderMode", "JsNeeded",
             "SeedFromSitemap", "FollowLinkedPages", "PathRules", "ReviewAfterPages", "PauseReason", "PauseDetail",
             "CreatedAt", "UpdatedAt")
        VALUES
            (@OwnerUserId, @Status, @SeedUrls, @AllowedHosts, @MaxDepth, @MaxUrls, @RenderMode, @JsNeeded,
             @SeedFromSitemap, @FollowLinkedPages, @PathRules, @ReviewAfterPages, @PauseReason, @PauseDetail,
             @CreatedAt, @UpdatedAt)
        RETURNING "Id"
        """;

    private const string SaveSql = $"""
        UPDATE "{TableNames.CrawlJobs}"
        SET "Status" = @Status, "UpdatedAt" = @UpdatedAt, "StartedAt" = @StartedAt, "CompletedAt" = @CompletedAt,
            "PathRules" = @PathRules, "ReviewAfterPages" = @ReviewAfterPages,
            "PauseReason" = @PauseReason, "PauseDetail" = @PauseDetail
        WHERE "Id" = @Id
        """;

    private const string CompleteSql = $"""
        UPDATE "{TableNames.CrawlJobs}" j
        SET "Status" = 'Completed', "CompletedAt" = @Now, "UpdatedAt" = @Now
        WHERE j."Status" = 'Running'
          AND NOT EXISTS (
              SELECT 1 FROM "{TableNames.DiscoveredUrls}" u
              WHERE u."CrawlJobId" = j."Id"
                AND (u."Status" = 'InFlight'
                     OR (u."Status" = 'Idle' AND (u."LastCrawlAt" IS NULL OR u."FailCount" > 0))))
          AND NOT EXISTS (
              SELECT 1 FROM "{TableNames.UrlFetches}" f
              JOIN "{TableNames.DiscoveredUrls}" u ON u."Id" = f."DiscoveredUrlId"
              WHERE u."CrawlJobId" = j."Id" AND f."ParsedAt" IS NULL AND f."ObjectName" IS NOT NULL)
        RETURNING j."Id"
        """;

    static PostgresCrawlJobRepository() => DapperTypeHandlers.Register();

    public async Task<CrawlJob?> Find(int id, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await connection.QuerySingleOrDefaultAsync<CrawlJob>(new CommandDefinition(GetSql, new { Id = id }, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<CrawlJob>> ListRecent(int limit, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var jobs = await connection.QueryAsync<CrawlJob>(new CommandDefinition(ListSql, new { Limit = limit }, cancellationToken: ct));
        return jobs.ToList();
    }

    public async Task<int> Insert(CrawlJob job, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await connection.ExecuteScalarAsync<int>(new CommandDefinition(InsertSql, Parameters(job), cancellationToken: ct));
    }

    public async Task Save(CrawlJob job, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(SaveSql, Parameters(job), cancellationToken: ct));
    }

    public async Task<IReadOnlyList<int>> CompleteFinishedJobs(DateTimeOffset now, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var ids = await connection.QueryAsync<int>(new CommandDefinition(CompleteSql, new { Now = now }, cancellationToken: ct));
        return ids.ToList();
    }

    private static object Parameters(CrawlJob job) => new
    {
        job.Id,
        job.OwnerUserId,
        Status = job.Status.ToString(),
        SeedUrls = JsonSerializer.Serialize(job.SeedUrls, JsonDefaults.Options),
        AllowedHosts = JsonSerializer.Serialize(job.AllowedHosts, JsonDefaults.Options),
        job.MaxDepth,
        job.MaxUrls,
        RenderMode = job.RenderMode.ToString(),
        job.JsNeeded,
        job.SeedFromSitemap,
        job.FollowLinkedPages,
        PathRules = JsonSerializer.Serialize(job.PathRules, JsonDefaults.Options),
        job.ReviewAfterPages,
        job.PauseReason,
        job.PauseDetail,
        job.CreatedAt,
        job.UpdatedAt,
        job.StartedAt,
        job.CompletedAt
    };
}
