using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SitemapCrawlState
{
    Crawled,
    NotCrawled,
    Blocked
}

/// <summary>A sitemap URL as the database knows it. A page id and status code mean that the URL was fetched.</summary>
public sealed record SitemapUrlRow
{
    public string Loc { get; init; } = string.Empty;
    public DateTimeOffset? LastModified { get; init; }
    public int? StatusCode { get; init; }
    public int? PageId { get; init; }
}

public sealed record SitemapCounts(int UrlCount, int CrawledCount, int NotInSitemapCount);

public sealed record SitemapSnapshot(IReadOnlyList<SitemapFile> Files, SitemapCounts Counts, PagedResult<SitemapUrlRow> Urls);

public sealed record SitemapUrlItem(string Loc, DateTimeOffset? LastModified, SitemapCrawlState CrawlState, int? StatusCode, int? PageId);

public sealed record SitemapOverview(
    string Host,
    IReadOnlyList<SitemapFile> Files,
    SitemapCounts Counts,
    PagedResult<SitemapUrlItem> Urls);
