using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IUrlFrontier
{
    /// <summary>Claims at most one due URL for each host. The URLs stay claimed until the lease ends.</summary>
    Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, CancellationToken ct);

    /// <summary>Records the fetch, the new URL state, and an optional new URL in one transaction.</summary>
    Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct);

    Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct);
}
