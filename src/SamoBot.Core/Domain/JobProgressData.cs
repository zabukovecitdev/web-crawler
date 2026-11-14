namespace SamoBot.Core.Domain;

/// <summary>The raw numbers of one job that the progress query reads. <see cref="CrawlProgress.From"/> turns them into the API shape.</summary>
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

    /// <summary>The delay of the host of the current URL. Without a URL in flight, the host of the first seed.</summary>
    public int CrawlDelayMs { get; init; } = 1000;

    /// <summary>True when the robots.txt of that host sets the delay.</summary>
    public bool DelayFromRobots { get; init; }

    public DateTimeOffset? NextAllowedAt { get; init; }

    public int Fetches { get; init; }
    public double? AvgResponseMs { get; init; }
    public long? MaxResponseMs { get; init; }
    public int Ok { get; init; }
    public int Redirect { get; init; }
    public int ClientError { get; init; }
    public int ServerError { get; init; }
}
