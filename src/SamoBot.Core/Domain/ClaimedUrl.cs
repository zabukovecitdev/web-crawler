namespace SamoBot.Core.Domain;

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

    public int CrawlDelayMs { get; init; } = 1000;

    public DateTimeOffset? HeldUntil { get; init; }

    public double? AvgResponseMs { get; init; }

    public int ConsecutiveFailures { get; init; }
}
