using FluentResults;
using SamoBot.Core.Common;

namespace SamoBot.Core.Domain;

/// <summary>A crawl job. The job owns the rules for its status transitions.</summary>
public sealed class CrawlJob
{
    public int Id { get; init; }
    public string? OwnerUserId { get; init; }
    public IReadOnlyList<string> SeedUrls { get; init; } = [];
    public IReadOnlyList<string> AllowedHosts { get; init; } = [];
    public int? MaxDepth { get; init; }
    public int? MaxUrls { get; init; }
    public bool UseJsRendering { get; init; }
    public bool RespectRobots { get; init; } = true;
    public DateTimeOffset CreatedAt { get; init; }

    public CrawlJobStatus Status { get; private set; } = CrawlJobStatus.Pending;
    public DateTimeOffset UpdatedAt { get; private set; }
    public DateTimeOffset? StartedAt { get; private set; }
    public DateTimeOffset? CompletedAt { get; private set; }

    /// <summary>Creates a pending job. Invalid seed URLs are dropped. The hosts of the valid seeds become the allowed hosts.</summary>
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
            UseJsRendering = request.UseJsRendering,
            RespectRobots = request.RespectRobots,
            CreatedAt = now,
            UpdatedAt = now
        });
    }

    /// <summary>Returns the valid seed URLs as depth-0 frontier entries.</summary>
    public IReadOnlyList<NewUrl> ToSeedUrls() =>
        SeedUrls
            .Select(seed => UrlNormalizer.TryClean(seed, out var clean) ? clean : null)
            .OfType<Uri>()
            .Select(clean => new NewUrl(Id, clean.AbsoluteUri, clean.AbsoluteUri, clean.Host, 0))
            .DistinctBy(seed => seed.NormalizedUrl)
            .ToList();

    public Result Start(DateTimeOffset now)
    {
        var result = Transition(Status is CrawlJobStatus.Pending or CrawlJobStatus.Paused, CrawlJobStatus.Running, now);
        if (result.IsSuccess)
        {
            StartedAt ??= now;
        }

        return result;
    }

    public Result Pause(DateTimeOffset now) =>
        Transition(Status is CrawlJobStatus.Running, CrawlJobStatus.Paused, now);

    public Result Cancel(DateTimeOffset now)
    {
        var result = Transition(Status is not CrawlJobStatus.Completed, CrawlJobStatus.Cancelled, now);
        if (result.IsSuccess)
        {
            CompletedAt = now;
        }

        return result;
    }

    public void Complete(DateTimeOffset now)
    {
        Status = CrawlJobStatus.Completed;
        CompletedAt = now;
        UpdatedAt = now;
    }

    private static string? HostOf(string seed) => UrlNormalizer.TryClean(seed, out var clean) ? clean!.Host : null;

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
