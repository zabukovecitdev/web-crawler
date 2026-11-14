using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

public sealed record SitemapFileDto(string Url, string? ParentUrl, SitemapKind Kind, int UrlCount, DateTimeOffset FetchedAt);

public sealed record SitemapUrlDto(string Loc, DateTimeOffset? LastModified, SitemapCrawlState CrawlState, int? StatusCode, int? PageId);

public sealed record SitemapUrlsDto(int Total, IReadOnlyList<SitemapUrlDto> Items);

/// <summary>The shape of SitemapDto in src/frontend/src/types/api.ts.</summary>
public sealed record SitemapDto(
    string Host,
    IReadOnlyList<SitemapFileDto> Files,
    int UrlCount,
    int CrawledCount,
    int NotInSitemapCount,
    SitemapUrlsDto Urls)
{
    public static SitemapDto From(SitemapOverview overview) => new(
        overview.Host,
        overview.Files.Select(file => new SitemapFileDto(file.Url, file.ParentUrl, file.Kind, file.UrlCount, file.FetchedAt)).ToList(),
        overview.Counts.UrlCount,
        overview.Counts.CrawledCount,
        overview.Counts.NotInSitemapCount,
        new SitemapUrlsDto(
            overview.Urls.Total,
            overview.Urls.Items.Select(item => new SitemapUrlDto(item.Loc, item.LastModified, item.CrawlState, item.StatusCode, item.PageId)).ToList()));
}
