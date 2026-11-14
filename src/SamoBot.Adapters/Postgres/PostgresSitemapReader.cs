using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// Read-only queries about the stored sitemaps of a host. A sitemap URL counts as crawled when a page of the host
/// with the same normalized URL has a fetch. When several jobs hold that page, the newest crawl wins.
/// </summary>
public sealed class PostgresSitemapReader(NpgsqlDataSource dataSource) : ISitemapReader
{
    private const string FilesSql = $"""
        SELECT "Host", "Url", "ParentUrl", "Kind", "UrlCount", "FetchedAt"
        FROM "{TableNames.SitemapFiles}" WHERE "Host" = @Host ORDER BY "Id"
        """;

    private const string CountsSql = $"""
        SELECT
            (SELECT count(*) FROM "{TableNames.SitemapUrls}" WHERE "Host" = @Host)::int AS "UrlCount",
            (SELECT count(*) FROM "{TableNames.SitemapUrls}" s
             WHERE s."Host" = @Host AND EXISTS (
                 SELECT 1 FROM "{TableNames.DiscoveredUrls}" u
                 WHERE u."Host" = s."Host" AND u."NormalizedUrl" = s."NormalizedLoc" AND u."LastFetchId" IS NOT NULL))::int AS "CrawledCount",
            (SELECT count(DISTINCT u."NormalizedUrl") FROM "{TableNames.DiscoveredUrls}" u
             WHERE u."Host" = @Host AND u."LastFetchId" IS NOT NULL AND NOT EXISTS (
                 SELECT 1 FROM "{TableNames.SitemapUrls}" s
                 WHERE s."Host" = u."Host" AND s."NormalizedLoc" = u."NormalizedUrl"))::int AS "NotInSitemapCount"
        """;

    private const string UrlsSql = $"""
        SELECT s."Loc", s."LastModified", d."StatusCode", d."Id" AS "PageId"
        FROM "{TableNames.SitemapUrls}" s
        LEFT JOIN LATERAL (
            SELECT u."Id", f."StatusCode"::int AS "StatusCode"
            FROM "{TableNames.DiscoveredUrls}" u
            JOIN "{TableNames.UrlFetches}" f ON f."Id" = u."LastFetchId"
            WHERE u."Host" = s."Host" AND u."NormalizedUrl" = s."NormalizedLoc"
            ORDER BY u."LastCrawlAt" DESC NULLS LAST, u."Id" DESC
            LIMIT 1) d ON true
        WHERE s."Host" = @Host
        ORDER BY s."Loc"
        LIMIT @Limit OFFSET @Offset
        """;

    static PostgresSitemapReader() => DapperTypeHandlers.Register();

    public async Task<SitemapSnapshot> Read(string host, int limit, int offset, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { Host = host, Limit = limit, Offset = offset };
        var rows = await connection.QueryAsync<SitemapFileRow>(new CommandDefinition(FilesSql, parameters, cancellationToken: ct));
        var files = rows.Select(row => row.ToFile()).ToList();
        if (files.Count == 0)
        {
            return new SitemapSnapshot(files, new SitemapCounts(0, 0, 0), new PagedResult<SitemapUrlRow>(0, []));
        }

        var counts = await connection.QuerySingleAsync<SitemapCountsRow>(new CommandDefinition(CountsSql, parameters, cancellationToken: ct));
        var urls = await connection.QueryAsync<SitemapUrlRow>(new CommandDefinition(UrlsSql, parameters, cancellationToken: ct));
        return new SitemapSnapshot(files, counts.ToCounts(), new PagedResult<SitemapUrlRow>(counts.UrlCount, urls.ToList()));
    }
}

/// <summary>The row of a stored sitemap file. Dapper fills the properties.</summary>
internal sealed record SitemapFileRow
{
    public string Host { get; init; } = string.Empty;
    public string Url { get; init; } = string.Empty;
    public string? ParentUrl { get; init; }
    public string Kind { get; init; } = string.Empty;
    public int UrlCount { get; init; }
    public DateTimeOffset FetchedAt { get; init; }

    public SitemapFile ToFile() => new(Host, Url, ParentUrl, Enum.Parse<SitemapKind>(Kind), UrlCount, FetchedAt);
}

/// <summary>The counts row of a host. Dapper fills the properties.</summary>
internal sealed record SitemapCountsRow
{
    public int UrlCount { get; init; }
    public int CrawledCount { get; init; }
    public int NotInSitemapCount { get; init; }

    public SitemapCounts ToCounts() => new(UrlCount, CrawledCount, NotInSitemapCount);
}
