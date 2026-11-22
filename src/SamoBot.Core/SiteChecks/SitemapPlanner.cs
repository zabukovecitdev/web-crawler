using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.SiteChecks;

public sealed class SitemapPlanner(ISitemapPlanReader reader, ISitemapStore store, TimeProvider timeProvider)
{
    public async Task<SitemapPlan> Ensure(string host, CancellationToken ct)
    {
        var status = await reader.Status(host, ct);
        var (state, enqueue) = SitemapPlanRules.Decide(status, timeProvider.GetUtcNow());
        if (enqueue)
        {
            await store.Enqueue(host, ct);
        }

        return new SitemapPlan(state, status.Files, status.Urls, status.CompletedAt);
    }
}
