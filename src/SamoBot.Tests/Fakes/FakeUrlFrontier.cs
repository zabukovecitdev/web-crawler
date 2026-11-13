using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeUrlFrontier(params ClaimedUrl[] due) : IUrlFrontier
{
    public List<(ClaimedUrl Url, UrlUpdate Update, FetchRecord? Fetch)> Completed { get; } = [];
    public List<(int JobId, IReadOnlyList<NewUrl> Seeds)> AddedSeeds { get; } = [];
    public (int BatchSize, TimeSpan Lease)? LastClaim { get; private set; }

    public Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, CancellationToken ct)
    {
        LastClaim = (batchSize, lease);
        return Task.FromResult<IReadOnlyList<ClaimedUrl>>(due);
    }

    public Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct)
    {
        Completed.Add((url, update, fetch));
        return Task.CompletedTask;
    }

    public Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct)
    {
        AddedSeeds.Add((crawlJobId, seeds));
        return Task.CompletedTask;
    }
}
