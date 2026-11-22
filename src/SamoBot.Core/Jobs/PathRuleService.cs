using FluentResults;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Core.Scope;

namespace SamoBot.Core.Jobs;

public sealed class PathRuleService(
    ICrawlJobRepository jobs,
    IPathRuleApplier applier,
    ICrawlEventPublisher events,
    TimeProvider timeProvider)
{
    public async Task<Result<PathRuleEffect>> Change(int jobId, IReadOnlyList<PathRule> rules, CancellationToken ct)
    {
        var job = await jobs.Find(jobId, ct);
        if (job is null)
        {
            return Result.Fail<PathRuleEffect>(new JobNotFoundError(jobId));
        }

        var problems = PathRuleValidator.Validate(rules);
        if (problems.Count > 0)
        {
            return Result.Fail<PathRuleEffect>(problems.Select(problem => new InvalidRequestError(problem)));
        }

        var changed = job.ChangePathRules(rules, timeProvider.GetUtcNow());
        if (changed.IsFailed)
        {
            return changed.ToResult<PathRuleEffect>();
        }

        await jobs.SavePathRules(job, ct);
        var effect = await applier.Apply(jobId, ct);
        await events.Publish(jobId, CrawlEventTypes.JobPathRulesChanged, new { stopped = effect.Stopped, resumed = effect.Resumed }, ct);
        return Result.Ok(effect);
    }
}
