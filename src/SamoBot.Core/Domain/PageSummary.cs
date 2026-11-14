using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum ContentState
{
    ParsedIndexed,
    ParsedWaitingForIndex,
    WaitingForParse,
    Redirect,
    Skipped,
    NotStored,
    Failed
}

public enum PageStateFilter
{
    All,
    Ok,
    Redirect,
    Error
}

public sealed record PageQuery(string? Query, PageStateFilter State, int Limit, int Offset);

public record PageSummary
{
    public int Id { get; init; }
    public string Url { get; init; } = string.Empty;
    public string Path { get; init; } = string.Empty;
    public string? Title { get; init; }
    public int? StatusCode { get; init; }
    public string? ContentType { get; init; }
    public long? ContentLength { get; init; }
    public long? ResponseTimeMs { get; init; }
    public int Depth { get; init; }
    public DateTimeOffset? LastCrawlAt { get; init; }
    public ContentState ContentState { get; init; }
}
