namespace SamoBot.Core.Domain;

public sealed record FetchRecord
{
    public required DateTimeOffset FetchedAt { get; init; }
    public required int StatusCode { get; init; }
    public string? ContentType { get; init; }
    public long? ContentLength { get; init; }
    public bool BodyTruncated { get; init; }
    public long? ResponseTimeMs { get; init; }
    public string? ObjectName { get; init; }
    public string? FinalUrl { get; init; }
    public ResponseHeaders ResponseHeaders { get; init; } = ResponseHeaders.None;
}
