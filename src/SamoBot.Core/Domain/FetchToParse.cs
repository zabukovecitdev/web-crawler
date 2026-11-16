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

    /// <summary>The Content-Type of the fetch. It selects how the stored body is read (HTML or Markdown).</summary>
    public string? ContentType { get; init; }
    public IReadOnlyList<string> AllowedHosts { get; init; } = [];
    public int? MaxDepth { get; init; }

    /// <summary>The response headers kept with the fetch (see <see cref="Directives.ResponseHeaderSelection"/>).</summary>
    public ResponseHeaders ResponseHeaders { get; init; } = ResponseHeaders.None;
}
