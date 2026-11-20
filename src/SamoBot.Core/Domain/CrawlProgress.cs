namespace SamoBot.Core.Domain;

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
    public const string AdaptiveSource = "adaptive";

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
        Source(data),
        NextRequest(data, now),
        data.AvgResponseMs is { } average ? Math.Round(average, 1) : null,
        data.MaxResponseMs,
        Rate(data, now),
        data.StartedAt,
        new CrawlStatusCounts(data.Ok, data.Redirect, data.ClientError, data.ServerError));

    private static string Source(JobProgressData data)
    {
        if (data.DelayIsAdaptive)
        {
            return AdaptiveSource;
        }

        return data.DelayFromRobots ? RobotsSource : DefaultSource;
    }

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
