using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Jobs;

public sealed class JobCompletionHandler(
    ICrawlJobRepository jobs,
    ICrawlEventPublisher events,
    TimeProvider timeProvider) : IWorkHandler
{
    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var completed = await jobs.CompleteFinishedJobs(timeProvider.GetUtcNow(), ct);
        foreach (var id in completed)
        {
            await events.Publish(id, CrawlEventTypes.JobCompleted, new { }, ct);
        }

        return 0;
    }
}
