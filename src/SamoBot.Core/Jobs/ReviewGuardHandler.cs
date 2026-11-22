using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Jobs;

public sealed class ReviewGuardHandler(
    IReviewPauseStore store,
    ICrawlEventPublisher events,
    TimeProvider timeProvider) : IWorkHandler
{
    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var paused = await store.PauseJobsAtTheReviewPoint(timeProvider.GetUtcNow(), ct);
        foreach (var id in paused)
        {
            await events.Publish(id, CrawlEventTypes.JobPaused, new { reason = PauseReasons.Review }, ct);
        }

        return 0;
    }
}
