namespace SamoBot.Core.Domain;

public enum AnalyticsRange
{
    All,
    Day,
    Week,
    Month
}

public enum AnalyticsBucket
{
    Hour,
    Day
}

/// <summary>A time window over UrlFetches.FetchedAt. A null From means no lower bound.</summary>
public sealed record AnalyticsWindow(DateTimeOffset? From, DateTimeOffset To)
{
    public static AnalyticsWindow For(AnalyticsRange range, DateTimeOffset now) => new(range switch
    {
        AnalyticsRange.Day => now.AddHours(-24),
        AnalyticsRange.Week => now.AddDays(-7),
        AnalyticsRange.Month => now.AddDays(-30),
        _ => (DateTimeOffset?)null
    }, now);
}

/// <summary>Parses the range and bucket names of the analytics endpoints. A null result means the name is unknown.</summary>
public static class AnalyticsNames
{
    /// <summary>The most buckets a timeseries returns. An older start is cut so the chart stays small.</summary>
    public const int MaxBuckets = 1000;

    public static AnalyticsRange? ParseRange(string? name) => name?.ToLowerInvariant() switch
    {
        null or "" or "all" => AnalyticsRange.All,
        "24h" => AnalyticsRange.Day,
        "7d" => AnalyticsRange.Week,
        "30d" => AnalyticsRange.Month,
        _ => null
    };

    /// <summary>A missing bucket is hour for the 24 hour range and day otherwise.</summary>
    public static AnalyticsBucket? ParseBucket(string? name, AnalyticsRange range) => name?.ToLowerInvariant() switch
    {
        null or "" => range == AnalyticsRange.Day ? AnalyticsBucket.Hour : AnalyticsBucket.Day,
        "hour" => AnalyticsBucket.Hour,
        "day" => AnalyticsBucket.Day,
        _ => null
    };

    /// <summary>The oldest bucket start a timeseries may begin at.</summary>
    public static DateTimeOffset Floor(DateTimeOffset to, AnalyticsBucket bucket) =>
        bucket == AnalyticsBucket.Hour ? to.AddHours(-MaxBuckets) : to.AddDays(-MaxBuckets);
}
