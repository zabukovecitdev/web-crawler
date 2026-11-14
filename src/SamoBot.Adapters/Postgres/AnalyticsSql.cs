namespace SamoBot.Adapters.Postgres;

/// <summary>
/// The SQL of the analytics read model. Every query reads UrlFetches in a FetchedAt window (IX_UrlFetches_FetchedAt)
/// and joins DiscoveredUrls by primary key. Values come in as parameters; no text here is built from input.
/// </summary>
internal static class AnalyticsSql
{
    private const string Failed = """(f."StatusCode" IS NULL OR f."StatusCode" < 200 OR f."StatusCode" >= 400)""";

    private const string InWindow = """(@From::timestamptz IS NULL OR f."FetchedAt" >= @From::timestamptz)""";

    private const string ForHost = """(@Host::text IS NULL OR u."Host" = @Host::text)""";

    private const string Source = $"""
        FROM "{TableNames.UrlFetches}" f
        JOIN "{TableNames.DiscoveredUrls}" u ON u."Id" = f."DiscoveredUrlId"
        """;

    private const string Latency = """
        percentile_cont(0.5) WITHIN GROUP (ORDER BY f."ResponseTimeMs"::float8) AS "P50ResponseMs",
        percentile_cont(0.95) WITHIN GROUP (ORDER BY f."ResponseTimeMs"::float8) AS "P95ResponseMs",
        max(f."ResponseTimeMs")::float8 AS "MaxResponseMs"
        """;

    private const string FailureRate = $"""COALESCE(count(*) FILTER (WHERE {Failed})::float8 / NULLIF(count(*), 0), 0) AS "FailureRate" """;

    public const string Overview = $"""
        SELECT count(DISTINCT u."Host")::int AS "Hosts",
               count(DISTINCT f."DiscoveredUrlId")::int AS "Pages",
               count(*) AS "Requests",
               COALESCE(sum(f."ContentLength"), 0)::bigint AS "Bytes",
               count(DISTINCT f."DiscoveredUrlId") FILTER (WHERE {Failed})::int AS "FailedPages",
               {FailureRate},
               {Latency}
        {Source}
        WHERE {InWindow}
        """;

    public const string Hosts = $"""
        SELECT u."Host" AS "Host",
               count(DISTINCT f."DiscoveredUrlId")::int AS "Pages",
               count(*) AS "Requests",
               {Latency},
               COALESCE(sum(f."ContentLength"), 0)::bigint AS "Bytes",
               {FailureRate},
               COALESCE(max(h."CrawlDelayMs"), 1000) AS "CrawlDelayMs"
        {Source}
        LEFT JOIN "{TableNames.Hosts}" h ON h."Host" = u."Host"
        WHERE {InWindow}
        GROUP BY u."Host"
        ORDER BY "Pages" DESC, u."Host"
        LIMIT 500
        """;

    public const string StatusClasses = $"""
        SELECT count(*) FILTER (WHERE f."StatusCode" BETWEEN 200 AND 299) AS "Ok",
               count(*) FILTER (WHERE f."StatusCode" BETWEEN 300 AND 399) AS "Redirect",
               count(*) FILTER (WHERE f."StatusCode" BETWEEN 400 AND 499) AS "ClientError",
               count(*) FILTER (WHERE f."StatusCode" BETWEEN 500 AND 599) AS "ServerError"
        {Source}
        WHERE {InWindow} AND {ForHost}
        """;

    /// <summary>The media type without parameters, lower case. Missing types group as unknown.</summary>
    public const string ContentTypes = $"""
        SELECT COALESCE(NULLIF(lower(btrim(split_part(f."ContentType", ';', 1))), ''), 'unknown') AS "ContentType",
               count(*) AS "Requests",
               false AS "Stored"
        {Source}
        WHERE {InWindow} AND {ForHost}
        GROUP BY 1
        ORDER BY count(*) DESC, 1
        LIMIT 10
        """;

    /// <summary>
    /// Buckets are UTC. The series runs from the first bucket of the window to the bucket of @To, one row per bucket,
    /// with zero where nothing was fetched. For the all range it starts at the oldest fetch, cut to @Floor.
    /// </summary>
    public const string Timeseries = $"""
        WITH base AS (
            SELECT COALESCE(@From::timestamptz, (SELECT min("FetchedAt") FROM "{TableNames.UrlFetches}")) AS "Start"
        ),
        bounds AS (
            SELECT CASE WHEN "Start" IS NULL THEN NULL
                        ELSE date_trunc(@Unit, GREATEST("Start", @Floor::timestamptz) AT TIME ZONE 'UTC') END AS "First",
                   date_trunc(@Unit, @To::timestamptz AT TIME ZONE 'UTC') AS "Last"
            FROM base
        ),
        counts AS (
            SELECT date_trunc(@Unit, f."FetchedAt" AT TIME ZONE 'UTC') AS "Bucket", count(*) AS "Requests"
            {Source}
            WHERE f."FetchedAt" >= (SELECT "First" AT TIME ZONE 'UTC' FROM bounds) AND {ForHost}
            GROUP BY 1
        )
        SELECT (g."Bucket" AT TIME ZONE 'UTC') AS "Bucket", COALESCE(c."Requests", 0) AS "Requests"
        FROM bounds b
        CROSS JOIN LATERAL generate_series(
            b."First", b."Last", CASE WHEN @Unit = 'hour' THEN interval '1 hour' ELSE interval '1 day' END) AS g("Bucket")
        LEFT JOIN counts c ON c."Bucket" = g."Bucket"
        ORDER BY g."Bucket"
        """;
}
