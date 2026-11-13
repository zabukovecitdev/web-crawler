using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

/// <summary>The SQL of the site read model. The ORDER BY clauses are fixed text chosen from the enum, never from input.</summary>
internal static class SiteSql
{
    private const string Ctes = $"""
        WITH stats AS (
            SELECT u."Host" AS "Host",
                   count(*) FILTER (WHERE u."LastFetchId" IS NOT NULL)::int AS "Pages",
                   count(*) FILTER (WHERE f."StatusCode" >= 400 OR f."StatusCode" < 200 OR u."FailReason" IS NOT NULL)::int AS "Failed",
                   avg(f."ResponseTimeMs")::float8 AS "AvgResponseMs",
                   max(u."LastCrawlAt") AS "LastCrawlAt",
                   max(u."CrawlJobId") FILTER (WHERE j."Status" = 'Running') AS "ActiveJobId",
                   (array_agg(j."Status" ORDER BY j."Id" DESC) FILTER (WHERE j."Id" IS NOT NULL))[1] AS "NewestJobStatus"
            FROM "{TableNames.DiscoveredUrls}" u
            LEFT JOIN "{TableNames.UrlFetches}" f ON f."Id" = u."LastFetchId"
            LEFT JOIN "{TableNames.CrawlJobs}" j ON j."Id" = u."CrawlJobId"
            GROUP BY u."Host"
        ),
        sites AS (
            SELECT s."Host", s."Pages", s."Failed", s."AvgResponseMs", s."LastCrawlAt", s."ActiveJobId",
                   COALESCE(h."CrawlDelayMs", 1000) AS "CrawlDelayMs",
                   CASE WHEN s."ActiveJobId" IS NOT NULL THEN 'Crawling'
                        WHEN s."NewestJobStatus" = 'Paused' THEN 'Paused'
                        WHEN s."Pages" > 0 THEN 'Crawled'
                        ELSE 'Never' END AS "Status"
            FROM stats s
            LEFT JOIN "{TableNames.Hosts}" h ON h."Host" = s."Host"
        )
        """;

    private const string Columns =
        """SELECT "Host", "Status", "Pages", "Failed", "AvgResponseMs", "CrawlDelayMs", "LastCrawlAt", "ActiveJobId" FROM sites""";

    private const string HostFilter = """WHERE (@Pattern::text IS NULL OR "Host" ILIKE @Pattern ESCAPE '\')""";

    private const string Paging = "LIMIT @Limit OFFSET @Offset";

    public const string Count = $"""{Ctes} SELECT count(*)::int FROM sites {HostFilter}""";

    public const string Find = $"""{Ctes} {Columns} WHERE "Host" = @Host""";

    public const string Overview = $"""
        {Ctes}
        SELECT (SELECT count(*) FROM sites)::int AS "Sites",
               COALESCE((SELECT sum("Pages") FROM sites), 0)::int AS "Pages",
               COALESCE((SELECT sum("Failed") FROM sites), 0)::int AS "Failed",
               (SELECT count(*) FROM sites WHERE "Status" = 'Crawling')::int AS "Crawling",
               (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY f."ResponseTimeMs"::float8)
                  FROM "{TableNames.UrlFetches}" f JOIN "{TableNames.DiscoveredUrls}" u ON u."LastFetchId" = f."Id") AS "MedianResponseMs",
               (SELECT percentile_cont(0.95) WITHIN GROUP (ORDER BY f."ResponseTimeMs"::float8)
                  FROM "{TableNames.UrlFetches}" f JOIN "{TableNames.DiscoveredUrls}" u ON u."LastFetchId" = f."Id") AS "P95ResponseMs"
        """;

    public const string FirstCrawl = $"""
        SELECT min(f."FetchedAt") AS "FirstCrawlAt"
        FROM "{TableNames.UrlFetches}" f
        JOIN "{TableNames.DiscoveredUrls}" u ON u."Id" = f."DiscoveredUrlId"
        WHERE u."Host" = @Host
        """;

    public const string StatusCodes = $"""
        SELECT f."StatusCode"::int AS "StatusCode", count(*)::int AS "Count"
        FROM "{TableNames.DiscoveredUrls}" u
        JOIN "{TableNames.UrlFetches}" f ON f."Id" = u."LastFetchId"
        WHERE u."Host" = @Host
        GROUP BY f."StatusCode"
        ORDER BY f."StatusCode"
        """;

    public const string ContentTypes = $"""
        SELECT COALESCE(NULLIF(lower(btrim(split_part(f."ContentType", ';', 1))), ''), 'unknown') AS "ContentType",
               count(*)::int AS "Count"
        FROM "{TableNames.DiscoveredUrls}" u
        JOIN "{TableNames.UrlFetches}" f ON f."Id" = u."LastFetchId"
        WHERE u."Host" = @Host
        GROUP BY 1
        ORDER BY count(*) DESC, 1
        """;

    public static string List(SiteSort sort) => sort switch
    {
        SiteSort.Pages => $"""{Ctes} {Columns} {HostFilter} ORDER BY "Pages" DESC, "Host" {Paging}""",
        SiteSort.Failed => $"""{Ctes} {Columns} {HostFilter} ORDER BY "Failed" DESC, "Host" {Paging}""",
        SiteSort.LastCrawl => $"""{Ctes} {Columns} {HostFilter} ORDER BY "LastCrawlAt" DESC NULLS LAST, "Host" {Paging}""",
        _ => $"""{Ctes} {Columns} {HostFilter} ORDER BY "Host" {Paging}"""
    };
}
