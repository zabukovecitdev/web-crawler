using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeSitemapStore : ISitemapStore
{
    public List<SitemapWalkResult> Results { get; } = [];
    public Queue<string> Claims { get; } = new();
    public int EnqueueFinishedCalls { get; private set; }
    public int Reservations { get; private set; }
    public TimeSpan Wait { get; set; } = TimeSpan.Zero;
    public List<(string Host, IReadOnlyList<SitemapFileContent> Files)> Completed { get; } = [];

    public Task EnqueueFinishedHosts(TimeSpan refreshAfter, CancellationToken ct)
    {
        EnqueueFinishedCalls++;
        return Task.CompletedTask;
    }

    public Task Enqueue(string host, CancellationToken ct) => Task.CompletedTask;

    public Task<SitemapClaim?> ClaimNext(TimeSpan lease, CancellationToken ct) =>
        Task.FromResult(Claims.TryDequeue(out var host) ? new SitemapClaim(host, TestData.Now, TestData.Now + lease) : null);

    public Task<TimeSpan> ReserveFetchSlot(string host, CancellationToken ct)
    {
        Reservations++;
        return Task.FromResult(Wait);
    }

    public Task<bool> Complete(SitemapClaim claim, SitemapWalkResult result, CancellationToken ct)
    {
        Completed.Add((claim.Host, result.Files));
        Results.Add(result);
        return Task.FromResult(true);
    }
}
