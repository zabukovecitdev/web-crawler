using Microsoft.Extensions.Caching.Memory;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

public sealed class RobotsGate(
    IMemoryCache cache,
    IRobotsStore store,
    IRobotsSource source,
    TimeProvider timeProvider)
{
    public const string UserAgent = "SamoBot";
    private static readonly TimeSpan BlockedTtl = TimeSpan.FromHours(1);

    public async Task<bool> IsAllowed(ClaimedUrl url, CancellationToken ct)
    {
        var rules = await RulesOrThrow(url.Host, ct);
        return rules.IsAllowed(new Uri(url.Url).PathAndQuery, UserAgent);
    }

    public async Task<RobotsRules> RulesFor(string host, CancellationToken ct)
    {
        try
        {
            return await RulesOrThrow(host, ct);
        }
        catch (BlockedAddressException)
        {
            return RobotsRules.DisallowAll(host, timeProvider.GetUtcNow(), BlockedTtl);
        }
    }

    private async Task<RobotsRules> RulesOrThrow(string host, CancellationToken ct)
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
        var stored = await store.Find(host, ct);
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
