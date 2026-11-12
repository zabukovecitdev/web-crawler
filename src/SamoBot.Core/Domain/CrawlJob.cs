using FluentResults;

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

    public Result Cancel(DateTimeOffset now) =>
        Transition(Status is not CrawlJobStatus.Completed, CrawlJobStatus.Cancelled, now);

    public void Complete(DateTimeOffset now)
    {
        Status = CrawlJobStatus.Completed;
        CompletedAt = now;
        UpdatedAt = now;
    }

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
