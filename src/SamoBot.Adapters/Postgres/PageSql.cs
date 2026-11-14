namespace SamoBot.Adapters.Postgres;

/// <summary>The SQL of the page read model. A page is a DiscoveredUrls row; its latest fetch is the row at LastFetchId.</summary>
internal static class PageSql
{
    private const string SummaryColumns = """
        u."Id", u."Url", u."Host", u."Depth", u."LastCrawlAt", u."NextCrawlAt", u."FailReason", u."CrawlJobId", u."UseJsRendering",
        d."Title", f."StatusCode"::int AS "StatusCode", f."ContentType", f."ContentLength", f."ResponseTimeMs", f."ObjectName",
        (d."Id" IS NOT NULL) AS "HasParsedDocument", (d."IndexedAt" IS NOT NULL) AS "Indexed"
        """;

    private const string From = $"""
        FROM "{TableNames.DiscoveredUrls}" u
        LEFT JOIN "{TableNames.UrlFetches}" f ON f."Id" = u."LastFetchId"
        LEFT JOIN "{TableNames.ParsedDocuments}" d ON d."UrlFetchId" = f."Id"
        """;

    private const string HostPagesWhere = """
        WHERE u."Host" = @Host
          AND (@Pattern::text IS NULL OR u."Url" ILIKE @Pattern ESCAPE '\' OR d."Title" ILIKE @Pattern ESCAPE '\')
          AND (@State = 'all'
               OR (@State = 'ok' AND f."StatusCode" BETWEEN 200 AND 299)
               OR (@State = 'redirect' AND f."StatusCode" BETWEEN 300 AND 399)
               OR (@State = 'error' AND (f."StatusCode" >= 400 OR f."StatusCode" < 200
                                         OR (f."Id" IS NULL AND u."FailReason" IS NOT NULL))))
        """;

    public const string ListHostPages = $"""
        SELECT {SummaryColumns}
        {From}
        {HostPagesWhere}
        ORDER BY u."LastCrawlAt" DESC NULLS LAST, u."Id"
        LIMIT @Limit OFFSET @Offset
        """;

    public const string CountHostPages = $"""SELECT count(*)::int {From} {HostPagesWhere}""";

    public const string Find = $"""
        SELECT {SummaryColumns},
               d."Description", d."Language", d."Canonical", d."RobotsDirectives", d."BodyText", d."Headings", d."Images",
               d."OpenGraphData", d."JsonLdData"
        {From}
        WHERE u."Id" = @Id
        """;

    public const string Fetches = $"""
        SELECT "FetchedAt", "StatusCode"::int AS "StatusCode", "ContentType", "ContentLength", "ResponseTimeMs", "FinalUrl"
        FROM "{TableNames.UrlFetches}"
        WHERE "DiscoveredUrlId" = @Id
        ORDER BY "FetchedAt" DESC, "Id" DESC
        LIMIT 100
        """;

    public const string ContentObjectName = $"""
        SELECT "ObjectName"
        FROM "{TableNames.UrlFetches}"
        WHERE "DiscoveredUrlId" = @Id AND "ObjectName" IS NOT NULL
        ORDER BY "FetchedAt" DESC, "Id" DESC
        LIMIT 1
        """;
}
