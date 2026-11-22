using SamoBot.Core.Domain;
using SamoBot.Core.Sitemaps;

namespace SamoBot.Core.SiteChecks;

public static class SitemapPlanRules
{
    public static (SitemapPlanState State, bool Enqueue) Decide(SitemapPlanStatus status, DateTimeOffset now)
    {
        if (status.CompletedAt is { } completed && now - completed <= SitemapBatchHandler.RefreshAfter)
        {
            return (status.Files > 0 ? SitemapPlanState.Ready : SitemapPlanState.None, false);
        }

        if (status.RequestedAt is not null && status.CompletedAt is null)
        {
            return (status.Attempts >= SitemapBatchHandler.MaxAttempts ? SitemapPlanState.Failed : SitemapPlanState.Pending, false);
        }

        return (SitemapPlanState.Pending, true);
    }
}
