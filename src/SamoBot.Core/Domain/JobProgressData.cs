namespace SamoBot.Core.Domain;

public sealed record JobProgressData
{
    public int JobId { get; init; }
    public string Status { get; init; } = string.Empty;
    public int? MaxUrls { get; init; }
    public DateTimeOffset? StartedAt { get; init; }
    public DateTimeOffset? CompletedAt { get; init; }

    public int Crawled { get; init; }
    public int Queued { get; init; }
    public int InFlight { get; init; }
    public int Failed { get; init; }

    public string? CurrentUrl { get; init; }
    public int? CurrentDepth { get; init; }

    public int CrawlDelayMs { get; init; } = 1000;

    public bool DelayFromRobots { get; init; }

    public bool DelayIsAdaptive { get; init; }

    public DateTimeOffset? NextAllowedAt { get; init; }

    public int Fetches { get; init; }
    public double? AvgResponseMs { get; init; }
    public long? MaxResponseMs { get; init; }
    public int Ok { get; init; }
    public int Redirect { get; init; }
    public int ClientError { get; init; }
    public int ServerError { get; init; }
}
