using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// Reads the live counters of one job with one query. The counts come from the frontier and from the fetches of the job.
/// A URL counts by its last fetch: crawled (2xx), failed (disabled with a reason other than redirect, robots or blocked-address; a blocked address counts as skipped, not failed),
/// queued (idle and not yet fetched, or waiting for a retry), in flight (leased).
/// </summary>
public sealed class PostgresJobProgressReader(NpgsqlDataSource dataSource) : IJobProgressReader
{
    private const string ProgressSql = $"""
        WITH job AS (
            SELECT "Id", "Status", "MaxUrls", "StartedAt", "CompletedAt", "AllowedHosts"::jsonb ->> 0 AS "FirstHost"
            FROM "{TableNames.CrawlJobs}" WHERE "Id" = @JobId
        ),
        frontier AS (
            SELECT
                count(*) FILTER (WHERE f."StatusCode" BETWEEN 200 AND 299)::int AS "Crawled",
                count(*) FILTER (WHERE d."Status" = 'Idle' AND (d."LastFetchId" IS NULL OR d."FailCount" > 0))::int AS "Queued",
                count(*) FILTER (WHERE d."Status" = 'InFlight')::int AS "InFlight",
                count(*) FILTER (WHERE d."Status" = 'Disabled' AND d."FailReason" IS NOT NULL
                                   AND d."FailReason" NOT IN ('redirect', 'robots', 'blocked-address'))::int AS "Failed"
            FROM "{TableNames.DiscoveredUrls}" d
            LEFT JOIN "{TableNames.UrlFetches}" f ON f."Id" = d."LastFetchId"
            WHERE d."CrawlJobId" = @JobId
        ),
        inflight AS (
            SELECT "Url", "Depth", "Host" FROM "{TableNames.DiscoveredUrls}"
            WHERE "CrawlJobId" = @JobId AND "Status" = 'InFlight'
            ORDER BY "StatusUpdatedAt" DESC NULLS LAST, "Id" DESC
            LIMIT 1
        ),
        fetches AS (
            SELECT
                count(*)::int AS "Fetches",
                avg(f."ResponseTimeMs")::float8 AS "AvgResponseMs",
                max(f."ResponseTimeMs")::bigint AS "MaxResponseMs",
                count(*) FILTER (WHERE f."StatusCode" BETWEEN 200 AND 299)::int AS "Ok",
                count(*) FILTER (WHERE f."StatusCode" BETWEEN 300 AND 399)::int AS "Redirect",
                count(*) FILTER (WHERE f."StatusCode" BETWEEN 400 AND 499)::int AS "ClientError",
                count(*) FILTER (WHERE f."StatusCode" BETWEEN 500 AND 599)::int AS "ServerError"
            FROM "{TableNames.UrlFetches}" f
            JOIN "{TableNames.DiscoveredUrls}" d ON d."Id" = f."DiscoveredUrlId"
            WHERE d."CrawlJobId" = @JobId AND d."FailReason" IS DISTINCT FROM 'blocked-address'
        ),
        target AS (
            SELECT COALESCE((SELECT "Host" FROM inflight), job."FirstHost") AS "Host" FROM job
        )
        SELECT job."Id" AS "JobId", job."Status", job."MaxUrls", job."StartedAt", job."CompletedAt",
               frontier."Crawled", frontier."Queued", frontier."InFlight", frontier."Failed",
               inflight."Url" AS "CurrentUrl", inflight."Depth" AS "CurrentDepth",
               COALESCE(h."CrawlDelayMs", 1000) AS "CrawlDelayMs",
               (r."CrawlDelayMs" IS NOT NULL) AS "DelayFromRobots",
               h."NextAllowedAt",
               fetches."Fetches", fetches."AvgResponseMs", fetches."MaxResponseMs",
               fetches."Ok", fetches."Redirect", fetches."ClientError", fetches."ServerError"
        FROM job
        CROSS JOIN frontier
        CROSS JOIN fetches
        CROSS JOIN target
        LEFT JOIN inflight ON true
        LEFT JOIN "{TableNames.Hosts}" h ON h."Host" = target."Host"
        LEFT JOIN "{TableNames.RobotsTxt}" r ON r."Host" = target."Host"
        """;

    public async Task<JobProgressData?> Read(int crawlJobId, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await connection.QuerySingleOrDefaultAsync<JobProgressData>(
            new CommandDefinition(ProgressSql, new { JobId = crawlJobId }, cancellationToken: ct));
    }
}
