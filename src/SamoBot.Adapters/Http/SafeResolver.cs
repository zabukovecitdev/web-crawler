using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

/// <summary>The allowed addresses of a host, and whether they came from the cache (a failed connect may then retry with fresh DNS).</summary>
public sealed record AllowedAddresses(IReadOnlyList<IPAddress> Addresses, bool FromCache);

/// <summary>
/// Gives the addresses that the crawler may connect to. Cached addresses (younger than <c>Crawler:DnsCacheMinutes</c>)
/// are used before DNS, and every address is checked with <see cref="IpAddressPolicy"/> on every use.
/// IP literals get the same check and are never cached.
/// </summary>
public sealed class SafeResolver(
    IDnsResolver dns,
    IHostAddressCache cache,
    IOptions<CrawlerNetworkOptions> options,
    TimeProvider timeProvider)
{
    /// <summary>Throws <see cref="BlockedAddressException"/> when the host has no allowed address.</summary>
    public async Task<AllowedAddresses> ResolveAllowed(string host, CancellationToken ct)
    {
        if (IsLiteral(host, out var literal))
        {
            return new AllowedAddresses(Filter(host, [literal]), FromCache: false);
        }

        var cached = await FreshAllowedFromCache(host, ct);
        return cached.Count > 0 ? new AllowedAddresses(cached, FromCache: true) : new AllowedAddresses(await Refresh(host, ct), FromCache: false);
    }

    /// <summary>Resolves with DNS, ignores the cache, stores the allowed addresses. When none is allowed it clears the cache and throws.</summary>
    public async Task<IReadOnlyList<IPAddress>> Refresh(string host, CancellationToken ct)
    {
        var all = await dns.Resolve(host, ct);
        if (all.Count == 0)
        {
            throw new SocketException((int)SocketError.HostNotFound);
        }

        var allowed = all.Where(IsAllowed).ToList();
        if (allowed.Count == 0)
        {
            await cache.Clear(host, ct);
            throw new BlockedAddressException(host);
        }

        await Remember(host, allowed, ct);
        return allowed;
    }

    /// <summary>True when every address of the host is allowed. The browser needs this, because it picks the address itself. DNS only, no cache.</summary>
    public async Task<bool> AllAllowed(string host, CancellationToken ct)
    {
        if (IsLiteral(host, out var literal))
        {
            return IsAllowed(literal);
        }

        return (await dns.Resolve(host, ct)).All(IsAllowed);
    }

    private bool IsAllowed(IPAddress address) => options.Value.AllowPrivateAddresses || IpAddressPolicy.IsAllowed(address);

    private List<IPAddress> Filter(string host, IEnumerable<IPAddress> addresses)
    {
        var allowed = addresses.Where(IsAllowed).ToList();
        return allowed.Count > 0 ? allowed : throw new BlockedAddressException(host);
    }

    private static bool IsLiteral(string host, out IPAddress address) => IPAddress.TryParse(host.Trim('[', ']'), out address!);

    /// <summary>The cached addresses that are still fresh and still allowed. Empty means: resolve again.</summary>
    private async Task<IReadOnlyList<IPAddress>> FreshAllowedFromCache(string host, CancellationToken ct)
    {
        var ttl = TimeSpan.FromMinutes(options.Value.DnsCacheMinutes);
        if (ttl <= TimeSpan.Zero)
        {
            return [];
        }

        var found = await cache.Find(host, ct);
        if (found is null || timeProvider.GetUtcNow() - found.ResolvedAt >= ttl)
        {
            return [];
        }

        return found.Addresses.Where(IsAllowed).ToList();
    }

    private Task Remember(string host, IReadOnlyList<IPAddress> allowed, CancellationToken ct) =>
        options.Value.DnsCacheMinutes > 0
            ? cache.Save(new ResolvedHost(host, allowed, timeProvider.GetUtcNow()), ct)
            : Task.CompletedTask;
}
