namespace SamoBot.Core.Domain;

/// <summary>The search document of a parsed page. The property names match the Meilisearch index.</summary>
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
}
