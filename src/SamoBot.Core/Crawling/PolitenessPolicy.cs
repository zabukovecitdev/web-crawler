using SamoBot.Core.Domain;

namespace SamoBot.Core.Crawling;

public sealed class PolitenessPolicy(TimeSpan maxAdaptiveDelay)
{
    public const double Alpha = 0.3;
    public const double SlowFactor = 5;
    public const int MaxExponent = 10;
    public static readonly TimeSpan MaxRetryAfter = TimeSpan.FromHours(1);
    public static readonly TimeSpan MaxBackoff = TimeSpan.FromMinutes(10);

    public HostPacingUpdate? Next(ClaimedUrl url, FetchResponse response)
    {
        if (response.IsBlocked)
        {
            return null;
        }

        var baseDelay = TimeSpan.FromMilliseconds(url.CrawlDelayMs);
        return IsHealthy(response) ? Healthy(url, response, baseDelay) : Failing(url, response, baseDelay);
    }

    private static bool IsHealthy(FetchResponse response) => response.StatusCode is >= 200 and < 500 and not 429;

    private HostPacingUpdate Healthy(ClaimedUrl url, FetchResponse response, TimeSpan baseDelay)
    {
        var average = Average(url.AvgResponseMs, response.ResponseTimeMs);
        return new HostPacingUpdate(Adaptive(baseDelay, average, 0), average, 0);
    }

    private HostPacingUpdate Failing(ClaimedUrl url, FetchResponse response, TimeSpan baseDelay)
    {
        var failures = url.ConsecutiveFailures + 1;
        var delay = Adaptive(baseDelay, url.AvgResponseMs, failures);
        if (response.StatusCode is 429 or 503)
        {
            delay = Max(delay, Throttled(baseDelay, failures, response.RetryAfter));
        }

        return new HostPacingUpdate(delay, url.AvgResponseMs, failures);
    }

    private static double Average(double? previous, long responseTimeMs) =>
        previous is { } average ? (Alpha * responseTimeMs) + ((1 - Alpha) * average) : responseTimeMs;

    private TimeSpan Adaptive(TimeSpan baseDelay, double? average, int failures)
    {
        var slow = TimeSpan.FromMilliseconds(SlowFactor * (average ?? 0));
        var wanted = Max(baseDelay, slow) * Doubling(failures);
        return Max(baseDelay, Min(wanted, maxAdaptiveDelay));
    }

    private static TimeSpan Throttled(TimeSpan baseDelay, int failures, TimeSpan? retryAfter) =>
        retryAfter is { } wait
            ? Min(Max(wait, TimeSpan.Zero), MaxRetryAfter)
            : Min(baseDelay * Doubling(failures), MaxBackoff);

    private static double Doubling(int failures) => Math.Pow(2, Math.Clamp(failures, 0, MaxExponent));

    private static TimeSpan Max(TimeSpan a, TimeSpan b) => a >= b ? a : b;

    private static TimeSpan Min(TimeSpan a, TimeSpan b) => a <= b ? a : b;
}
