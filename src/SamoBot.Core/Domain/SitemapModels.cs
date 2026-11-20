using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SitemapKind
{
    Index,
    UrlSet
}

public sealed record SitemapEntry(string Loc, string NormalizedLoc, DateTimeOffset? LastModified);

public sealed record SitemapFile(string Host, string Url, string? ParentUrl, SitemapKind Kind, int UrlCount, DateTimeOffset FetchedAt);

public sealed record SitemapFileContent(SitemapFile File, IReadOnlyList<SitemapEntry> Urls);

public sealed record SitemapDownload(int StatusCode, byte[]? Content)
{
    public bool IsSuccess => StatusCode is >= 200 and <= 299 && Content is { Length: > 0 };
}

public sealed record SitemapClaim(string Host, DateTimeOffset ClaimedAt, DateTimeOffset LeaseUntil);

public sealed record SitemapWalkResult(IReadOnlyList<SitemapFileContent> Files, bool Gone);
