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
}
