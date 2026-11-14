namespace SamoBot.Core.Domain;

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
}
