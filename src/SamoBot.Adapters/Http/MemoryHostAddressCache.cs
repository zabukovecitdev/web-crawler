using System.Collections.Concurrent;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

/// <summary>
/// A small in-memory layer in front of another cache, so that a connect does not query PostgreSQL each time.
/// It keeps what it saw or saved and never decides freshness: the caller checks <see cref="ResolvedHost.ResolvedAt"/>.
/// A name that the inner cache did not know is remembered as unknown until it is saved.
/// </summary>
public sealed class MemoryHostAddressCache(IHostAddressCache inner) : IHostAddressCache
{
    public const int MaxEntries = 10_000;

    private readonly ConcurrentDictionary<string, ResolvedHost?> _entries = new(StringComparer.OrdinalIgnoreCase);

    public async Task<ResolvedHost?> Find(string host, CancellationToken ct)
    {
        if (_entries.TryGetValue(host, out var known))
        {
            return known;
        }

        var found = await inner.Find(host, ct);
        Remember(host, found);
        return found;
    }

    public async Task Save(ResolvedHost resolved, CancellationToken ct)
    {
        Remember(resolved.Host, resolved);
        await inner.Save(resolved, ct);
    }

    public async Task Clear(string host, CancellationToken ct)
    {
        Remember(host, null);
        await inner.Clear(host, ct);
    }

    private void Remember(string host, ResolvedHost? value)
    {
        if (_entries.Count >= MaxEntries)
        {
            _entries.Clear();
        }

        _entries[host] = value;
    }
}
