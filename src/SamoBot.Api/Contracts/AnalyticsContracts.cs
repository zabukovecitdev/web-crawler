using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

public sealed record AnalyticsQuery(AnalyticsRange Range, string? Host, AnalyticsBucket Bucket);

public static class AnalyticsQueries
{
    public static AnalyticsQuery? Parse(string? range, string? host, string? bucket)
    {
        var parsedRange = AnalyticsNames.ParseRange(range);
        if (parsedRange is null)
        {
            return null;
        }

        var parsedBucket = AnalyticsNames.ParseBucket(bucket, parsedRange.Value);
        return parsedBucket is null ? null : new AnalyticsQuery(parsedRange.Value, CleanHost(host), parsedBucket.Value);
    }

    private static string? CleanHost(string? host) => string.IsNullOrWhiteSpace(host) ? null : host.Trim().ToLowerInvariant();
}
