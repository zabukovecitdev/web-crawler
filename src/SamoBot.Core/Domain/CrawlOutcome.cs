using SamoBot.Core.Common;

namespace SamoBot.Core.Domain;

/// <summary>The result of one crawl. Each outcome knows the new state of its URL.</summary>
public abstract record CrawlOutcome
{
    public abstract UrlUpdate ToUpdate(ClaimedUrl url, DateTimeOffset now);

    protected static UrlUpdate Disable(ClaimedUrl url, string reason, NewUrl? newUrl = null) =>
        new(UrlStatus.Disabled, null, url.FailCount, reason, newUrl);
}

public sealed record Succeeded(TimeSpan RecrawlAfter) : CrawlOutcome
{
    public override UrlUpdate ToUpdate(ClaimedUrl url, DateTimeOffset now) =>
        new(UrlStatus.Idle, now + RecrawlAfter, 0, null, null);
}

public sealed record RetryLater(string Reason, TimeSpan Delay) : CrawlOutcome
{
    public override UrlUpdate ToUpdate(ClaimedUrl url, DateTimeOffset now) =>
        new(UrlStatus.Idle, now + Delay, url.FailCount + 1, Reason, null);
}

public sealed record Disabled(string Reason) : CrawlOutcome
{
    public override UrlUpdate ToUpdate(ClaimedUrl url, DateTimeOffset now) =>
        Disable(url, Reason);
}

public sealed record Redirected(string TargetUrl) : CrawlOutcome
{
    public const string Reason = "redirect";

    public override UrlUpdate ToUpdate(ClaimedUrl url, DateTimeOffset now) =>
        Disable(url, Reason, ResolveTarget(url));

    private NewUrl? ResolveTarget(ClaimedUrl url)
    {
        if (!Uri.TryCreate(new Uri(url.Url), TargetUrl, out var target) || !IsWeb(target))
        {
            return null;
        }

        if (!UrlNormalizer.TryClean(target.AbsoluteUri, out var normalized) || normalized is null)
        {
            return null;
        }

        return new NewUrl(url.CrawlJobId, target.AbsoluteUri, normalized.AbsoluteUri, normalized.Authority, url.Depth);
    }

    private static bool IsWeb(Uri uri) => uri.Scheme is "http" or "https";
}
