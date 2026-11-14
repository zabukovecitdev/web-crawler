using System.Text.Json.Serialization;

namespace SamoBot.Adapters.Search;

/// <summary>A search result as Meilisearch returns it: the stored fields and the highlighted, cropped copy.</summary>
internal sealed class IndexedHit
{
    public int DiscoveredUrlId { get; init; }
    public string Host { get; init; } = string.Empty;
    public string Url { get; init; } = string.Empty;
    public string Title { get; init; } = string.Empty;
    public string Language { get; init; } = string.Empty;
    public DateTimeOffset? FetchedAt { get; init; }

    [JsonPropertyName("_formatted")]
    public FormattedFields? Formatted { get; init; }

    internal sealed class FormattedFields
    {
        public string? BodyText { get; init; }
        public string? Description { get; init; }
    }
}
