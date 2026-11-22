using FluentResults;
using SamoBot.Core.Common;
using SamoBot.Core.Scope;

namespace SamoBot.Core.Domain;

public sealed class CrawlJob
{
    public int Id { get; init; }
    public string? OwnerUserId { get; init; }
    public IReadOnlyList<string> SeedUrls { get; init; } = [];
    public IReadOnlyList<string> AllowedHosts { get; init; } = [];
    public int? MaxDepth { get; init; }
    public int? MaxUrls { get; init; }
    public RenderMode RenderMode { get; init; } = RenderMode.Auto;
    public bool JsNeeded { get; init; }
    public bool SeedFromSitemap { get; init; }
    public bool FollowLinkedPages { get; init; } = true;
    public DateTimeOffset CreatedAt { get; init; }

    public IReadOnlyList<PathRule> PathRules { get; private set; } = [];
    public int? ReviewAfterPages { get; private set; }
    public string? PauseReason { get; private set; }
    public string? PauseDetail { get; private set; }
    public CrawlJobStatus Status { get; private set; } = CrawlJobStatus.Pending;
    public DateTimeOffset UpdatedAt { get; private set; }
    public DateTimeOffset? StartedAt { get; private set; }
    public DateTimeOffset? CompletedAt { get; private set; }

    public static Result<CrawlJob> Create(NewCrawlJob request, DateTimeOffset now)
    {
        var seeds = request.SeedUrls.Where(seed => !string.IsNullOrWhiteSpace(seed)).ToList();
        var hosts = seeds.Select(HostOf).OfType<string>().Distinct(StringComparer.OrdinalIgnoreCase).ToList();
        if (hosts.Count == 0)
        {
            return Result.Fail("At least one valid seed URL is required.");
        }

        return Result.Ok(new CrawlJob
        {
            OwnerUserId = request.OwnerUserId,
            SeedUrls = seeds,
            AllowedHosts = hosts,
            MaxDepth = request.MaxDepth,
            MaxUrls = request.MaxUrls,
            RenderMode = request.RenderMode,
            JsNeeded = request.JsNeeded,
            SeedFromSitemap = request.SeedFromSitemap,
            FollowLinkedPages = request.FollowLinkedPages || !request.SeedFromSitemap,
            PathRules = PinnedStartPages.Apply(request.PathRules, StartPaths(seeds)),
            ReviewAfterPages = request.SeedFromSitemap ? null : request.ReviewAfterPages,
            CreatedAt = now,
            UpdatedAt = now
        });
    }

    public Result ChangePathRules(IReadOnlyList<PathRule> rules, DateTimeOffset now)
    {
        if (Status is not (CrawlJobStatus.Pending or CrawlJobStatus.Running or CrawlJobStatus.Paused))
        {
            return Result.Fail($"The folders of a {Status} crawl cannot be changed.");
        }

        PathRules = PinnedStartPages.Apply(rules, StartPaths(SeedUrls));
        UpdatedAt = now;
        return Result.Ok();
    }

    public Result PauseFor(string reason, string? detail, DateTimeOffset now)
    {
        var result = Transition(Status is CrawlJobStatus.Running, CrawlJobStatus.Paused, now);
        if (result.IsSuccess)
        {
            PauseReason = reason;
            PauseDetail = detail;
        }

        return result;
    }

    public bool NeedsBrowser => RenderModes.NeedsBrowser(RenderMode, JsNeeded);

    public bool IsPausedFor(string reason) => Status is CrawlJobStatus.Paused && PauseReason == reason;

    public IReadOnlyList<NewUrl> ToSeedUrls() =>
        SeedUrls
            .Select(seed => UrlNormalizer.TryClean(seed, out var clean) ? clean : null)
            .OfType<Uri>()
            .Select(clean => new NewUrl(Id, clean.AbsoluteUri, clean.AbsoluteUri, clean.Authority, 0))
            .DistinctBy(seed => seed.NormalizedUrl)
            .ToList();

    public Result Start(DateTimeOffset now)
    {
        var wasReview = IsPausedFor(PauseReasons.Review);
        var result = Transition(Status is CrawlJobStatus.Pending or CrawlJobStatus.Paused, CrawlJobStatus.Running, now);
        if (result.IsSuccess)
        {
            StartedAt ??= now;
            PauseReason = null;
            PauseDetail = null;
            ReviewAfterPages = wasReview ? null : ReviewAfterPages;
        }

        return result;
    }

    public Result Reprocess(DateTimeOffset now)
    {
        var result = Transition(Status is CrawlJobStatus.Completed, CrawlJobStatus.Running, now);
        if (result.IsSuccess)
        {
            CompletedAt = null;
        }

        return result;
    }

    public Result Pause(DateTimeOffset now)
    {
        var result = Transition(Status is CrawlJobStatus.Running, CrawlJobStatus.Paused, now);
        if (result.IsSuccess)
        {
            PauseReason = null;
            PauseDetail = null;
        }

        return result;
    }

    public Result Cancel(DateTimeOffset now)
    {
        var result = Transition(Status is not CrawlJobStatus.Completed, CrawlJobStatus.Cancelled, now);
        if (result.IsSuccess)
        {
            CompletedAt = now;
        }

        return result;
    }

    public Result Reopen(DateTimeOffset now, bool wholeJob)
    {
        if (Status is CrawlJobStatus.Draft or CrawlJobStatus.Pending)
        {
            return Result.Fail("The crawl has not started yet.");
        }

        if (Status is CrawlJobStatus.Cancelled && !wholeJob)
        {
            return Result.Fail("This crawl was cancelled. Start a new crawl for this link, or recrawl the whole job.");
        }

        if (Status is not (CrawlJobStatus.Running or CrawlJobStatus.Paused))
        {
            Status = CrawlJobStatus.Running;
            CompletedAt = null;
        }

        UpdatedAt = now;
        return Result.Ok();
    }

    public void Complete(DateTimeOffset now)
    {
        Status = CrawlJobStatus.Completed;
        CompletedAt = now;
        UpdatedAt = now;
    }

    private static IEnumerable<string> StartPaths(IEnumerable<string> seeds) =>
        seeds
            .Select(seed => UrlNormalizer.TryClean(seed, out var clean) ? clean!.AbsolutePath : null)
            .OfType<string>();

    private static string? HostOf(string seed) => UrlNormalizer.TryClean(seed, out var clean) ? clean!.Authority : null;

    private Result Transition(bool isAllowed, CrawlJobStatus target, DateTimeOffset now)
    {
        if (!isAllowed)
        {
            return Result.Fail($"A job in status {Status} cannot change to {target}.");
        }

        Status = target;
        UpdatedAt = now;
        return Result.Ok();
    }
}
