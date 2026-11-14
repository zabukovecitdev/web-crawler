using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Sitemaps;

/// <summary>
/// The sitemap loop. It queues the hosts of completed jobs, claims one host under a lease, and refreshes its sitemaps.
/// A host that failed keeps its queue row, so another run picks it up when the lease ends (the lease is the backoff).
/// After <see cref="MaxAttempts"/> claims without a finished walk the host waits for the next weekly refresh.
/// </summary>
public sealed class SitemapBatchHandler(ISitemapStore store, SitemapRefresher refresher, SitemapEnqueueSchedule schedule) : IWorkHandler
{
    public const int MaxAttempts = 5;

    public static readonly TimeSpan RefreshAfter = TimeSpan.FromDays(7);

    /// <summary>
    /// A walk fetches at most 50 files, and each fetch waits at most 60 s for the host delay and takes at most 60 s to download:
    /// 50 x (60 s + 60 s) = 100 minutes, plus the robots.txt fetch. Two hours leave room, so a running walk keeps its lease.
    /// </summary>
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
