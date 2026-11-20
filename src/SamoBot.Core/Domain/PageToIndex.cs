using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

public sealed record PageToIndex
{
    public required int PageId { get; init; }
    public required int UrlFetchId { get; init; }
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
    public PagePolicy Policy { get; init; } = PagePolicy.Default;
}
