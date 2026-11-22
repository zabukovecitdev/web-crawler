using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeUrlFrontier(params ClaimedUrl[] due) : IUrlFrontier
{
    public List<(ClaimedUrl Url, UrlUpdate Update, FetchRecord? Fetch)> Completed { get; } = [];
    public List<(int JobId, IReadOnlyList<NewUrl> Seeds)> AddedSeeds { get; } = [];
    public List<(int JobId, IReadOnlyList<string> MediaTypes)> Reprocessed { get; } = [];
    public (int BatchSize, TimeSpan Lease, TimeSpan Hold)? LastClaim { get; private set; }
    public bool? LastBrowserAvailable { get; private set; }

    public Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, TimeSpan hold, bool browserAvailable, CancellationToken ct)
    {
        LastClaim = (batchSize, lease, hold);
        LastBrowserAvailable = browserAvailable;
        return Task.FromResult<IReadOnlyList<ClaimedUrl>>(due);
    }

    public Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct)
    {
        Completed.Add((url, update, fetch));
        return Task.CompletedTask;
    }

    public List<(int JobId, RequeueScope Scope)> Requeued { get; } = [];
    public int RequeueResult { get; set; } = 1;

    public Task<int> Requeue(int crawlJobId, RequeueScope scope, CancellationToken ct)
    {
        Requeued.Add((crawlJobId, scope));
        return Task.FromResult(RequeueResult);
    }

    public Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct)
    {
        AddedSeeds.Add((crawlJobId, seeds));
        return Task.CompletedTask;
    }

    public Task<RequeuedWork> RequeueForReprocess(int crawlJobId, IReadOnlyList<string> storedMediaTypes, CancellationToken ct)
    {
        Reprocessed.Add((crawlJobId, storedMediaTypes));
        return Task.FromResult(new RequeuedWork(1, 2));
    }
}
