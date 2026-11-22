using SamoBot.Core.Browser;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Jobs;

public sealed class BrowserGuardHandler(
    BrowserAvailability browser,
    IBrowserPauseStore store,
    ICrawlEventPublisher events,
    TimeProvider timeProvider) : IWorkHandler
{
    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var status = await browser.Current(ct);
        var now = timeProvider.GetUtcNow();
        if (status.Available)
        {
            await Announce(await store.ResumeJobsPausedForBrowser(now, ct), CrawlEventTypes.JobStarted, new { resumed = true, reason = PauseReasons.BrowserUnavailable }, ct);
        }
        else
        {
            await Announce(await store.PauseJobsNeedingBrowser(now, ct), CrawlEventTypes.JobPaused, new { reason = PauseReasons.BrowserUnavailable }, ct);
        }

        return 0;
    }

    private async Task Announce(IReadOnlyList<int> jobIds, string eventType, object payload, CancellationToken ct)
    {
        foreach (var id in jobIds)
        {
            await events.Publish(id, eventType, payload, ct);
        }
    }
}
