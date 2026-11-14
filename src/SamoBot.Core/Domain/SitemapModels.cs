using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SitemapKind
{
    Index,
    UrlSet
}

/// <summary>One URL of a sitemap. The normalized form is the key to the crawled pages.</summary>
public sealed record SitemapEntry(string Loc, string NormalizedLoc, DateTimeOffset? LastModified);

public sealed record SitemapFile(string Host, string Url, string? ParentUrl, SitemapKind Kind, int UrlCount, DateTimeOffset FetchedAt);

/// <summary>A stored sitemap file and its URLs. An index has no URLs of its own; its children are separate files.</summary>
public sealed record SitemapFileContent(SitemapFile File, IReadOnlyList<SitemapEntry> Urls);

/// <summary>The bytes of a downloaded sitemap, still compressed if the server sent them compressed.</summary>
public sealed record SitemapDownload(int StatusCode, byte[]? Content)
{
    public bool IsSuccess => StatusCode is >= 200 and <= 299 && Content is { Length: > 0 };
}

/// <summary>
/// A host claimed from the sitemap queue. <paramref name="LeaseUntil"/> is the ownership token: it is the lease timestamp
/// stored with the queue row, and <c>Complete</c> only writes while the row still holds exactly this value.
/// </summary>
public sealed record SitemapClaim(string Host, DateTimeOffset ClaimedAt, DateTimeOffset LeaseUntil);

/// <summary>
/// The outcome of a sitemap walk. <paramref name="Gone"/> is true when the walk downloaded at least one URL and
/// every download answered 404 or 410, so the stored files of the host are stale and may be deleted.
/// </summary>
public sealed record SitemapWalkResult(IReadOnlyList<SitemapFileContent> Files, bool Gone);
