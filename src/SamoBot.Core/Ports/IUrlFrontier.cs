using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IUrlFrontier
{
    /// <summary>
    /// Claims at most one due URL for each host. The URLs stay claimed until the lease ends. Each claimed host is held for
    /// <paramref name="hold"/> (the longest fetch plus a margin), so that no second request goes to it while this one is in flight.
    /// <see cref="Complete"/> releases the hold. The hold must not be longer than the lease.
    /// </summary>
    Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, TimeSpan hold, CancellationToken ct);

    /// <summary>Records the fetch, the new URL state, and an optional new URL in one transaction.</summary>
    Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct);

    /// <summary>
    /// Puts links of a job back in the queue, due now and with a clean failure count, so that they are fetched again.
    /// A link that is in flight is left alone. Returns the number of links that were queued.
    /// </summary>
    Task<int> Requeue(int crawlJobId, RequeueScope scope, CancellationToken ct);

    Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct);

    /// <summary>
    /// Gives the work of a job back to the stages, in one transaction. A URL whose last fetch was a success with a body of a
    /// type in <paramref name="storedMediaTypes"/> that was not stored is due now and waits for its first fetch again.
    /// The last stored fetch of each other URL waits for the parser again (the parser then clears its index mark).
    /// </summary>
    Task<RequeuedWork> RequeueForReprocess(int crawlJobId, IReadOnlyList<string> storedMediaTypes, CancellationToken ct);
}
