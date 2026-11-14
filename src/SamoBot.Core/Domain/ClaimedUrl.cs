namespace SamoBot.Core.Domain;

/// <summary>A frontier URL that a worker holds under a lease.</summary>
public sealed record ClaimedUrl
{
    public required int Id { get; init; }
    public required int CrawlJobId { get; init; }
    public required string Host { get; init; }
    public required string Url { get; init; }
    public required string NormalizedUrl { get; init; }
    public required int Depth { get; init; }
    public required int FailCount { get; init; }
    public required bool UseJsRendering { get; init; }
    public required bool RespectRobots { get; init; }

    /// <summary>The delay of the host after this claim. The claim query fills it from the host row.</summary>
    public int CrawlDelayMs { get; init; } = 1000;
}
