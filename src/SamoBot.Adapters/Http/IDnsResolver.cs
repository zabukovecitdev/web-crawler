using System.Net;

namespace SamoBot.Adapters.Http;

/// <summary>Resolves a host name to all of its addresses. A small seam so that tests can fake DNS.</summary>
public interface IDnsResolver
{
    Task<IReadOnlyList<IPAddress>> Resolve(string host, CancellationToken ct);
}

public sealed class SystemDnsResolver : IDnsResolver
{
    public async Task<IReadOnlyList<IPAddress>> Resolve(string host, CancellationToken ct) =>
        await Dns.GetHostAddressesAsync(host, ct);
}
