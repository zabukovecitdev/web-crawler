namespace SamoBot.Core.Domain;

public sealed record AnalyticsOverview
{
    public int Hosts { get; init; }
    public int Pages { get; init; }
    public long Requests { get; init; }
    public long Bytes { get; init; }
    public int FailedPages { get; init; }
    public double FailureRate { get; init; }
    public double? P50ResponseMs { get; init; }
    public double? P95ResponseMs { get; init; }
    public double? MaxResponseMs { get; init; }
    public DateTimeOffset? From { get; init; }
    public DateTimeOffset? To { get; init; }
}

public sealed record HostAnalytics
{
    public string Host { get; init; } = string.Empty;
    public int Pages { get; init; }
    public long Requests { get; init; }
    public double? P50ResponseMs { get; init; }
    public double? P95ResponseMs { get; init; }
    public double? MaxResponseMs { get; init; }
    public long Bytes { get; init; }
    public double FailureRate { get; init; }
    public int CrawlDelayMs { get; init; }
}

public sealed record StatusClassCounts
{
    public long Ok { get; init; }
    public long Redirect { get; init; }
    public long ClientError { get; init; }
    public long ServerError { get; init; }
}

/// <summary>A media type and how many fetches returned it. Stored is true when the crawler keeps that type.</summary>
public sealed record AnalyticsContentType(string ContentType, long Requests, bool Stored);

public sealed record TimeseriesPoint
{
    public DateTimeOffset Bucket { get; init; }
    public long Requests { get; init; }
}
