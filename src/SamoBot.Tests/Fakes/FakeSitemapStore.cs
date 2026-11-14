using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeSitemapStore : ISitemapStore
{
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

    public Task<string?> ClaimNext(TimeSpan lease, CancellationToken ct) =>
        Task.FromResult(Claims.TryDequeue(out var host) ? host : null);

    public Task<TimeSpan> ReserveFetchSlot(string host, CancellationToken ct)
    {
        Reservations++;
        return Task.FromResult(Wait);
    }

    public Task Complete(string host, IReadOnlyList<SitemapFileContent> files, CancellationToken ct)
    {
        Completed.Add((host, files));
        return Task.CompletedTask;
    }
}
