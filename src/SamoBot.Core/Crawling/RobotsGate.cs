using Microsoft.Extensions.Caching.Memory;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

/// <summary>Decides whether robots.txt allows a URL. The rules come from the cache, then the store, then the source.</summary>
public sealed class RobotsGate(
    IMemoryCache cache,
    IRobotsStore store,
    IRobotsSource source,
    TimeProvider timeProvider)
{
    public const string UserAgent = "SamoBot";

    public async Task<bool> IsAllowed(ClaimedUrl url, CancellationToken ct)
    {
        if (!url.RespectRobots)
        {
            return true;
        }

        var rules = await RulesFor(url.Host, ct);
        return rules.IsAllowed(new Uri(url.Url).PathAndQuery, UserAgent);
    }

    /// <summary>Returns the rules of a host. The dashboard uses this too.</summary>
    public async Task<RobotsRules> RulesFor(string host, CancellationToken ct)
    {
        var key = $"robots:{host}";
        if (cache.TryGetValue(key, out RobotsRules? cached) && cached is not null)
        {
            return cached;
        }

        var rules = await LoadFresh(host, ct);
        Remember(key, rules);
        return rules;
    }

    private async Task<RobotsRules> LoadFresh(string host, CancellationToken ct)
    {
        var stored = await store.Get(host, ct);
        if (stored is not null && stored.ExpiresAt > timeProvider.GetUtcNow())
        {
            return stored;
        }

        var downloaded = await source.Download(host, ct);
        await store.Save(downloaded, ct);
        return downloaded;
    }

    private void Remember(string key, RobotsRules rules)
    {
        var lifetime = rules.ExpiresAt - timeProvider.GetUtcNow();
        if (lifetime > TimeSpan.Zero)
        {
            cache.Set(key, rules, lifetime);
        }
    }
}
