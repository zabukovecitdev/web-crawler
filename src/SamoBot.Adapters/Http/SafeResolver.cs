using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Http;

public sealed record AllowedAddresses(IReadOnlyList<IPAddress> Addresses, bool FromCache);

public sealed class SafeResolver(
    IDnsResolver dns,
    IHostAddressCache cache,
    IOptions<CrawlerNetworkOptions> options,
    TimeProvider timeProvider)
{
    public async Task<AllowedAddresses> ResolveAllowed(string host, CancellationToken ct)
    {
        if (IsLiteral(host, out var literal))
        {
            return new AllowedAddresses(Filter(host, [literal]), FromCache: false);
        }

        var cached = await FreshAllowedFromCache(host, ct);
        return cached.Count > 0 ? new AllowedAddresses(cached, FromCache: true) : new AllowedAddresses(await Refresh(host, ct), FromCache: false);
    }

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
