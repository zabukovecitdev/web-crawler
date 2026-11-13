using FluentResults;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Jobs;

/// <summary>The use cases of a crawl job. The <see cref="CrawlJob"/> decides which transitions are valid.</summary>
public sealed class CrawlJobService(
    ICrawlJobRepository jobs,
    IUrlFrontier frontier,
    ICrawlEventPublisher events,
    TimeProvider timeProvider)
{
    public async Task<Result<CrawlJob>> Create(NewCrawlJob request, CancellationToken ct)
    {
        var created = CrawlJob.Create(request, timeProvider.GetUtcNow());
        if (created.IsFailed)
        {
            return created;
        }

        var job = created.Value;
        var id = await jobs.Insert(job, ct);
        var saved = await jobs.Find(id, ct) ?? job;
        await events.Publish(id, CrawlEventTypes.JobCreated, new { seedCount = job.SeedUrls.Count }, ct);
        return Result.Ok(saved);
    }

    public async Task<Result> Start(int id, CancellationToken ct)
    {
        var loaded = await Load(id, ct);
        if (loaded.IsFailed)
        {
            return loaded.ToResult();
        }

        var job = loaded.Value;
        var started = job.Start(timeProvider.GetUtcNow());
        if (started.IsFailed)
        {
            return started;
        }

        // The seeds go in before the job turns Running. A failed insert leaves a Pending job that can start again.
        await frontier.AddSeeds(job.Id, job.ToSeedUrls(), ct);
        await jobs.Save(job, ct);
        await events.Publish(id, CrawlEventTypes.JobStarted, new { at = job.StartedAt }, ct);
        return Result.Ok();
    }

    public Task<Result> Pause(int id, CancellationToken ct) =>
        Change(id, (job, now) => job.Pause(now), CrawlEventTypes.JobPaused, ct);

    public Task<Result> Cancel(int id, CancellationToken ct) =>
        Change(id, (job, now) => job.Cancel(now), CrawlEventTypes.JobCancelled, ct);

    private async Task<Result> Change(
        int id,
        Func<CrawlJob, DateTimeOffset, Result> transition,
        string eventType,
        CancellationToken ct)
    {
        var loaded = await Load(id, ct);
        if (loaded.IsFailed)
        {
            return loaded.ToResult();
        }

        var changed = transition(loaded.Value, timeProvider.GetUtcNow());
        if (changed.IsFailed)
        {
            return changed;
        }

        await jobs.Save(loaded.Value, ct);
        await events.Publish(id, eventType, new { }, ct);
        return Result.Ok();
    }

    private async Task<Result<CrawlJob>> Load(int id, CancellationToken ct)
    {
        var job = await jobs.Find(id, ct);
        return job is null ? Result.Fail<CrawlJob>($"Job {id} was not found.") : Result.Ok(job);
    }
}
