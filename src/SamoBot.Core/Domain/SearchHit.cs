namespace SamoBot.Core.Domain;

/// <summary>The search document of a parsed page. The property names match the Meilisearch index.</summary>
public sealed record SearchHit
{
    public string Id { get; init; } = string.Empty;
    public int UrlFetchId { get; init; }
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
    /// <summary>Unix seconds after which the page is hidden from search (unavailable_after); <see cref="NoEnd"/> without one.</summary>
    public long IndexUntil { get; init; } = NoEnd;
    /// <summary>The longest snippet in characters; -1 for no limit, 0 for no snippet.</summary>
    public int SnippetMaxLength { get; init; } = -1;

    /// <summary>The end of 9999: search filters on "indexUntil &gt; now", so a page without an end needs a value far ahead.</summary>
    public const long NoEnd = 253402300799;
}
