using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISitemapStore
{
    Task EnqueueFinishedHosts(TimeSpan refreshAfter, CancellationToken ct);

    Task Enqueue(string host, CancellationToken ct);

    Task<SitemapClaim?> ClaimNext(TimeSpan lease, CancellationToken ct);

    Task<TimeSpan> ReserveFetchSlot(string host, CancellationToken ct);

    Task<bool> Complete(SitemapClaim claim, SitemapWalkResult result, CancellationToken ct);
}
