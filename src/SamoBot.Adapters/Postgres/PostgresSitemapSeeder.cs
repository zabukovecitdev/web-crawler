using Dapper;
using Npgsql;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

public sealed class PostgresSitemapSeeder(NpgsqlDataSource dataSource, TimeProvider timeProvider) : ISitemapSeeder
{
    private static readonly string SeedSql = $"""
        WITH job AS (
            SELECT {PathRuleSql.UseBrowser} AS "UseJsRendering", "PathRules"::jsonb AS rules,
                   COALESCE("MaxUrls", 2147483647) AS max_urls
            FROM "{TableNames.CrawlJobs}" WHERE "Id" = @JobId
        ),
        picked AS (
            SELECT DISTINCT ON (s."NormalizedLoc") s."Loc", s."NormalizedLoc"
            FROM "{TableNames.SitemapUrls}" s, job
            WHERE s."Host" = @Host
              AND {PathRuleSql.Allows("job.rules", PathRuleSql.PathOf("s.\"NormalizedLoc\""))}
            ORDER BY s."NormalizedLoc", s."Id"
        ),
        numbered AS (
            SELECT p.*, row_number() OVER (ORDER BY p."NormalizedLoc") AS rn FROM picked p
        ),
        inserted AS (
            INSERT INTO "{TableNames.DiscoveredUrls}"
                ("Host", "Url", "NormalizedUrl", "Status", "NextCrawlAt", "CrawlJobId", "Depth", "UseJsRendering")
            SELECT @Host, n."Loc", n."NormalizedLoc", 'Idle', @Now, @JobId, 0, job."UseJsRendering"
            FROM numbered n, job
            WHERE n.rn <= GREATEST(job.max_urls - (SELECT count(*) FROM "{TableNames.DiscoveredUrls}" WHERE "CrawlJobId" = @JobId), 0)
            ON CONFLICT ("CrawlJobId", "NormalizedUrl") DO NOTHING
            RETURNING 1
        )
        SELECT count(*)::int FROM inserted
        """;

    public async Task<int> SeedFromSitemap(int crawlJobId, string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { JobId = crawlJobId, Host = host, Now = timeProvider.GetUtcNow() };
        return await connection.ExecuteScalarAsync<int>(new CommandDefinition(SeedSql, parameters, cancellationToken: ct));
    }
}
