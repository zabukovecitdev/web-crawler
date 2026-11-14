using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeHostAddressCache : IHostAddressCache
{
    public Dictionary<string, ResolvedHost> Entries { get; } = [];

    public int Saves { get; private set; }

    public int Clears { get; private set; }

    public Task<ResolvedHost?> Find(string host, CancellationToken ct) =>
        Task.FromResult(Entries.GetValueOrDefault(host));

    public Task Save(ResolvedHost resolved, CancellationToken ct)
    {
        Saves++;
        Entries[resolved.Host] = resolved;
        return Task.CompletedTask;
    }

    public Task Clear(string host, CancellationToken ct)
    {
        Clears++;
        Entries.Remove(host);
        return Task.CompletedTask;
    }
}
