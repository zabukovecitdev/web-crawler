using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;
using SamoBot.Core.Common;

namespace SamoBot.Adapters.Http;

/// <summary>Resolves a host and keeps only the addresses that <see cref="IpAddressPolicy"/> allows. IP literals get the same check.</summary>
public sealed class SafeResolver(IDnsResolver dns, IOptions<CrawlerNetworkOptions> options)
{
    /// <summary>The allowed addresses of a host. Throws <see cref="BlockedAddressException"/> when there is none.</summary>
    public async Task<IReadOnlyList<IPAddress>> ResolveAllowed(string host, CancellationToken ct)
    {
        var allowed = (await Resolve(host, ct)).Where(IsAllowed).ToList();
        return allowed.Count > 0 ? allowed : throw new BlockedAddressException(host);
    }

    /// <summary>True when every address of the host is allowed. The browser needs this, because it picks the address itself.</summary>
    public async Task<bool> AllAllowed(string host, CancellationToken ct)
    {
        var all = await Resolve(host, ct);
        return all.All(IsAllowed);
    }

    private bool IsAllowed(IPAddress address) => options.Value.AllowPrivateAddresses || IpAddressPolicy.IsAllowed(address);

    private async Task<IReadOnlyList<IPAddress>> Resolve(string host, CancellationToken ct)
    {
        if (IPAddress.TryParse(host.Trim('[', ']'), out var literal))
        {
            return [literal];
        }

        var addresses = await dns.Resolve(host, ct);
        return addresses.Count > 0 ? addresses : throw new SocketException((int)SocketError.HostNotFound);
    }
}
