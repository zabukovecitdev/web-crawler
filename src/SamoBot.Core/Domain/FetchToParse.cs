namespace SamoBot.Core.Domain;

public sealed record FetchToParse
{
    public required int FetchId { get; init; }
    public required int UrlId { get; init; }
    public required int CrawlJobId { get; init; }
    public required string Url { get; init; }
    public required int Depth { get; init; }
    public required string ObjectName { get; init; }

    public string? ContentType { get; init; }
    public IReadOnlyList<string> AllowedHosts { get; init; } = [];
    public int? MaxDepth { get; init; }
    public IReadOnlyList<PathRule> PathRules { get; init; } = [];
    public bool FollowLinkedPages { get; init; } = true;
    public bool CanEscalate { get; init; }

    public ResponseHeaders ResponseHeaders { get; init; } = ResponseHeaders.None;
}
