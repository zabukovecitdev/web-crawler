using SamoBot.Core.Domain;

namespace SamoBot.Core.Crawling;

/// <summary>The only place that maps a fetch response to a crawl outcome.</summary>
public static class OutcomeClassifier
{
    public const int MaxFailures = 5;
    public static readonly TimeSpan RecrawlInterval = TimeSpan.FromDays(1);

    public static CrawlOutcome Classify(FetchResponse response, int failCount) => response.StatusCode switch
    {
        >= 200 and <= 299 => new Succeeded(RecrawlInterval),
        301 or 302 or 303 or 307 or 308 when HasLocation(response) => new Redirected(response.RedirectLocation!),
        0 or 429 or (>= 500 and <= 599) => ClassifyFailure(response, failCount),
        var code => new Disabled($"http-{code}")
    };

    private static CrawlOutcome ClassifyFailure(FetchResponse response, int failCount)
    {
        if (failCount + 1 >= MaxFailures)
        {
            return new Disabled("max-failures");
        }

        var reason = response.StatusCode == 0 ? response.Error ?? "network-error" : $"http-{response.StatusCode}";
        return new RetryLater(reason, BackoffPolicy.DelayFor(failCount));
    }

    private static bool HasLocation(FetchResponse response) => !string.IsNullOrEmpty(response.RedirectLocation);
}
