using System.Text.Json.Serialization;

namespace SamoBot.Adapters.Search;

internal sealed class IndexedHit
{
    public int DiscoveredUrlId { get; init; }
    public string Host { get; init; } = string.Empty;
    public string Url { get; init; } = string.Empty;
    public string Title { get; init; } = string.Empty;
    public string Language { get; init; } = string.Empty;
    public DateTimeOffset? FetchedAt { get; init; }
    public int SnippetMaxLength { get; init; } = -1;

    [JsonPropertyName("_formatted")]
    public FormattedFields? Formatted { get; init; }

    internal sealed class FormattedFields
    {
        public string? BodyText { get; init; }
        public string? Description { get; init; }
    }
}
