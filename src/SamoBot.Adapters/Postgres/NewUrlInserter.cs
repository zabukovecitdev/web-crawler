using System.Text.Json;
using Dapper;
using Npgsql;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// The one place that adds URLs to the frontier. It applies the scope of the job:
/// the allowed hosts and the maximum number of URLs. It also adds the host rows that the claim query needs.
/// </summary>
internal static class NewUrlInserter
{
    private const string InsertSql = $"""
        WITH job AS (
            SELECT "UseJsRendering", "RespectRobots", "AllowedHosts"::jsonb AS allowed,
                   COALESCE("MaxUrls", 2147483647) AS max_urls
            FROM "{TableNames.CrawlJobs}" WHERE "Id" = @JobId
        ),
        incoming AS (
            SELECT item->>'url' AS url, item->>'normalizedUrl' AS normalized,
                   item->>'host' AS host, (item->>'depth')::int AS depth, ord
            FROM jsonb_array_elements(@Links::jsonb) WITH ORDINALITY AS e(item, ord)
        ),
        fresh AS (
            SELECT DISTINCT ON (i.normalized) i.*
            FROM incoming i, job
            WHERE jsonb_exists(job.allowed, i.host)
              AND NOT EXISTS (SELECT 1 FROM "{TableNames.DiscoveredUrls}" d
                              WHERE d."CrawlJobId" = @JobId AND d."NormalizedUrl" = i.normalized)
            ORDER BY i.normalized, i.ord
        ),
        numbered AS (
            SELECT f.*, row_number() OVER (ORDER BY f.ord) AS rn FROM fresh f
        ),
        inserted AS (
            INSERT INTO "{TableNames.DiscoveredUrls}"
                ("Host", "Url", "NormalizedUrl", "Status", "NextCrawlAt", "CrawlJobId", "Depth", "UseJsRendering", "RespectRobots")
            SELECT n.host, n.url, n.normalized, 'Idle', @Now, @JobId, n.depth, job."UseJsRendering", job."RespectRobots"
            FROM numbered n, job
            WHERE n.rn <= GREATEST(job.max_urls - (SELECT count(*) FROM "{TableNames.DiscoveredUrls}" WHERE "CrawlJobId" = @JobId), 0)
            ORDER BY n.ord
            ON CONFLICT ("CrawlJobId", "NormalizedUrl") DO NOTHING
            RETURNING "Host"
        )
        INSERT INTO "{TableNames.Hosts}" ("Host", "NextAllowedAt") SELECT DISTINCT "Host", @Now FROM inserted ON CONFLICT DO NOTHING
        """;

    public static Task Insert(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        int crawlJobId,
        IReadOnlyList<NewUrl> urls,
        DateTimeOffset now,
        CancellationToken ct)
    {
        if (urls.Count == 0)
        {
            return Task.CompletedTask;
        }

        var parameters = new { JobId = crawlJobId, Links = JsonSerializer.Serialize(urls, JsonDefaults.Options), Now = now };
        return connection.ExecuteAsync(new CommandDefinition(InsertSql, parameters, transaction, cancellationToken: ct));
    }
}
