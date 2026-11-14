using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>The sitemap work queue (one row for each host) and the stored sitemap files.</summary>
public interface ISitemapStore
{
    /// <summary>Queues every host that has a completed job and no sitemap refresh for <paramref name="refreshAfter"/>.</summary>
    Task EnqueueFinishedHosts(TimeSpan refreshAfter, CancellationToken ct);

    /// <summary>Queues one host now, for an on-demand refresh. A request made while a refresh runs queues another refresh.</summary>
    Task Enqueue(string host, CancellationToken ct);

    /// <summary>Takes the oldest queued host under a lease. Returns null when no host is due.</summary>
    Task<SitemapClaim?> ClaimNext(TimeSpan lease, CancellationToken ct);

    /// <summary>Books the next fetch slot of a host and returns how long the caller waits for it. This uses the delay of the host.</summary>
    Task<TimeSpan> ReserveFetchSlot(string host, CancellationToken ct);

    /// <summary>
    /// Finishes the queue row of a claim. The stored files of the host are replaced when the walk found files or the sitemaps are gone;
    /// a walk with no files and only transient failures keeps the old ones. Returns false, and writes nothing, when the lease of the
    /// claim was taken over by another worker.
    /// </summary>
    Task<bool> Complete(SitemapClaim claim, SitemapWalkResult result, CancellationToken ct);
}
