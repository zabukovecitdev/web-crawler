using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Sitemaps;

public sealed class SitemapBatchHandler(ISitemapStore store, SitemapRefresher refresher, SitemapEnqueueSchedule schedule) : IWorkHandler
{
    public const int MaxAttempts = 5;

    public static readonly TimeSpan RefreshAfter = TimeSpan.FromDays(7);

    public static readonly TimeSpan Lease = TimeSpan.FromHours(2);

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        if (schedule.TryStart())
        {
            await store.EnqueueFinishedHosts(RefreshAfter, ct);
        }

        var claim = await store.ClaimNext(Lease, ct);
        if (claim is null)
        {
            return 0;
        }

        await refresher.Refresh(claim, ct);
        return 1;
    }
}
