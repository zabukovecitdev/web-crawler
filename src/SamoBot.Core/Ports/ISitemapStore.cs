using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>The sitemap work queue (one row for each host) and the stored sitemap files.</summary>
public interface ISitemapStore
{
    /// <summary>Queues every host that has a completed job and no sitemap refresh for <paramref name="refreshAfter"/>.</summary>
    Task EnqueueFinishedHosts(TimeSpan refreshAfter, CancellationToken ct);

    /// <summary>Queues one host now, for an on-demand refresh.</summary>
    Task Enqueue(string host, CancellationToken ct);

    /// <summary>Takes the oldest queued host under a lease. Returns null when no host is due.</summary>
    Task<string?> ClaimNext(TimeSpan lease, CancellationToken ct);

    /// <summary>Books the next fetch slot of a host and returns how long the caller waits for it. This uses the delay of the host.</summary>
    Task<TimeSpan> ReserveFetchSlot(string host, CancellationToken ct);

    /// <summary>Replaces the stored files of a host and finishes its queue row. With no files the old ones stay.</summary>
    Task Complete(string host, IReadOnlyList<SitemapFileContent> files, CancellationToken ct);
}
