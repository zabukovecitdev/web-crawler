using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

/// <summary>The parsed query of an analytics request.</summary>
public sealed record AnalyticsQuery(AnalyticsRange Range, string? Host, AnalyticsBucket Bucket);

/// <summary>Turns the query string of the analytics endpoints into a query. A null result means 400.</summary>
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
