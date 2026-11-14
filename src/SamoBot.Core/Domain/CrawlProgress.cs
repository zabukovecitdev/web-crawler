namespace SamoBot.Core.Domain;

/// <summary>The progress of a job, as the dashboard reads it. The property names match the TypeScript type <c>CrawlProgress</c>.</summary>
public sealed record CrawlProgress(
    int JobId,
    string Status,
    int Crawled,
    int Queued,
    int InFlight,
    int Failed,
    int? MaxUrls,
    string? CurrentUrl,
    int? CurrentDepth,
    int CrawlDelayMs,
    string DelaySource,
    DateTimeOffset? NextRequestAt,
    double? AvgResponseMs,
    long? MaxResponseMs,
    double? PagesPerMinute,
    DateTimeOffset? StartedAt,
    CrawlStatusCounts StatusCounts)
{
    public const string RobotsSource = "robots.txt";
    public const string DefaultSource = "default";

    /// <summary>The rate counts fetches over the time since the start. The time has a floor of one minute, so that the first seconds give no spike.</summary>
    public static CrawlProgress From(JobProgressData data, DateTimeOffset now) => new(
        data.JobId,
        data.Status,
        data.Crawled,
        data.Queued,
        data.InFlight,
        data.Failed,
        data.MaxUrls,
        data.CurrentUrl,
        data.CurrentDepth,
        data.CrawlDelayMs,
        data.DelayFromRobots ? RobotsSource : DefaultSource,
        NextRequest(data, now),
        data.AvgResponseMs is { } average ? Math.Round(average, 1) : null,
        data.MaxResponseMs,
        Rate(data, now),
        data.StartedAt,
        new CrawlStatusCounts(data.Ok, data.Redirect, data.ClientError, data.ServerError));

    // Only a running job makes a next request, and only when the host is not yet due.
    private static DateTimeOffset? NextRequest(JobProgressData data, DateTimeOffset now) =>
        data.Status == nameof(CrawlJobStatus.Running) && data.NextAllowedAt > now ? data.NextAllowedAt : null;

    private static double? Rate(JobProgressData data, DateTimeOffset now)
    {
        if (data.StartedAt is not { } started)
        {
            return null;
        }

        var minutes = Math.Max(((data.CompletedAt ?? now) - started).TotalMinutes, 1);
        return Math.Round(data.Fetches / minutes, 1);
    }
}

public sealed record CrawlStatusCounts(int Ok, int Redirect, int ClientError, int ServerError);
