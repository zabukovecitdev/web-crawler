namespace SamoBot.Core.Domain;

/// <summary>One row in the fetch history of a URL.</summary>
public sealed record FetchRecord
{
    public required DateTimeOffset FetchedAt { get; init; }
    public required int StatusCode { get; init; }
    public string? ContentType { get; init; }
    public long? ContentLength { get; init; }
    public long? ResponseTimeMs { get; init; }
    public string? ObjectName { get; init; }
    public string? FinalUrl { get; init; }
}
