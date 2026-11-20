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

public static class AnalyticsNames
{
    public const int MaxBuckets = 1000;

    public static AnalyticsRange? ParseRange(string? name) => name?.ToLowerInvariant() switch
    {
        null or "" or "all" => AnalyticsRange.All,
        "24h" => AnalyticsRange.Day,
        "7d" => AnalyticsRange.Week,
        "30d" => AnalyticsRange.Month,
        _ => null
    };

    public static AnalyticsBucket? ParseBucket(string? name, AnalyticsRange range) => name?.ToLowerInvariant() switch
    {
        null or "" => range == AnalyticsRange.Day ? AnalyticsBucket.Hour : AnalyticsBucket.Day,
        "hour" => AnalyticsBucket.Hour,
        "day" => AnalyticsBucket.Day,
        _ => null
    };

    public static DateTimeOffset Floor(DateTimeOffset to, AnalyticsBucket bucket) =>
        bucket == AnalyticsBucket.Hour ? to.AddHours(-MaxBuckets) : to.AddDays(-MaxBuckets);
}
