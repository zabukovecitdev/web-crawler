using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

/// <summary>Keeps the sites overview for a short time. It scans every URL, so one load at a time serves all callers.</summary>
public sealed class SiteOverviewCache(TimeProvider time, TimeSpan ttl) : IDisposable
{
    public static readonly TimeSpan DefaultTtl = TimeSpan.FromSeconds(30);

    private readonly SemaphoreSlim _gate = new(1, 1);
    private SitesOverview? _value;
    private DateTimeOffset _loadedAt;

    public static SiteOverviewCache Disabled => new(TimeProvider.System, TimeSpan.Zero);

    public async Task<SitesOverview> GetOrLoad(Func<Task<SitesOverview>> load)
    {
        await _gate.WaitAsync();
        try
        {
            var now = time.GetUtcNow();
            if (_value is null || ttl <= TimeSpan.Zero || now - _loadedAt >= ttl)
            {
                _value = await load();
                _loadedAt = now;
            }

            return _value;
        }
        finally
        {
            _gate.Release();
        }
    }

    public void Dispose() => _gate.Dispose();
}
