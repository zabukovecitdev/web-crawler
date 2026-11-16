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

    /// <summary>The delay of the host after this claim. The claim query fills it from the host row.</summary>
    public int CrawlDelayMs { get; init; } = 1000;

    /// <summary>When the in-flight hold of the host ends. Set by the claim. Null: no hold is known, so completing keeps the later wait.</summary>
    public DateTimeOffset? HeldUntil { get; init; }

    /// <summary>The moving average of the response time of the host, null before the first healthy fetch.</summary>
    public double? AvgResponseMs { get; init; }

    /// <summary>The timeouts, network errors, 5xx and 429 in a row at the host.</summary>
    public int ConsecutiveFailures { get; init; }
}
