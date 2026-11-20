using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IUrlFrontier
{
    Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, TimeSpan hold, CancellationToken ct);

    Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct);

    Task<int> Requeue(int crawlJobId, RequeueScope scope, CancellationToken ct);

    Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct);

    Task<RequeuedWork> RequeueForReprocess(int crawlJobId, IReadOnlyList<string> storedMediaTypes, CancellationToken ct);
}
