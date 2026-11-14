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

    Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct);
}
