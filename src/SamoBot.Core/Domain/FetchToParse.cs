namespace SamoBot.Core.Domain;

/// <summary>A stored fetch that waits for the parser, with the job scope that limits its links.</summary>
public sealed record FetchToParse
{
    public required int FetchId { get; init; }
    public required int UrlId { get; init; }
    public required int CrawlJobId { get; init; }
    public required string Url { get; init; }
    public required int Depth { get; init; }
    public required string ObjectName { get; init; }
    public IReadOnlyList<string> AllowedHosts { get; init; } = [];
    public int? MaxDepth { get; init; }

    /// <summary>The X-Robots-Tag header of the fetch, one value per line. Null without the header.</summary>
    public string? XRobotsTag { get; init; }
}
