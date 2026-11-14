using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Sitemaps;

/// <summary>
/// The sitemap loop. It queues the hosts of completed jobs, claims one host under a lease, and refreshes its sitemaps.
/// A host that failed keeps its queue row, so another run picks it up when the lease ends.
/// </summary>
public sealed class SitemapBatchHandler(ISitemapStore store, SitemapRefresher refresher) : IWorkHandler
{
    public static readonly TimeSpan RefreshAfter = TimeSpan.FromDays(7);
    public static readonly TimeSpan Lease = TimeSpan.FromMinutes(30);

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        await store.EnqueueFinishedHosts(RefreshAfter, ct);
        var host = await store.ClaimNext(Lease, ct);
        if (host is null)
        {
            return 0;
        }

        await refresher.Refresh(host, ct);
        return 1;
    }
}
