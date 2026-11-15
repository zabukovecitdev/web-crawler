using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

/// <summary>A parsed page that waits for the search index, with the policy that decides whether it is indexed.</summary>
public sealed record PageToIndex
{
    public required int PageId { get; init; }
    public required int UrlFetchId { get; init; }
    /// <summary>The id of the discovered URL, which is the page id of the dashboard.</summary>
    public int DiscoveredUrlId { get; init; }
    public string Host { get; init; } = string.Empty;
    public string Url { get; init; } = string.Empty;
    public DateTimeOffset? FetchedAt { get; init; }
    public string Title { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public string Keywords { get; init; } = string.Empty;
    public string Author { get; init; } = string.Empty;
    public string Language { get; init; } = string.Empty;
    public string Canonical { get; init; } = string.Empty;
    public string BodyText { get; init; } = string.Empty;
    /// <summary>What the site allows for the page. Pages parsed before policies were stored have the default policy.</summary>
    public PagePolicy Policy { get; init; } = PagePolicy.Default;
}
