using System.Net;

namespace SamoBot.Core.Ports;

/// <summary>The addresses that a host name resolved to, and when. The cache keeps this so that a connect need not resolve DNS each time.</summary>
public sealed record ResolvedHost(string Host, IReadOnlyList<IPAddress> Addresses, DateTimeOffset ResolvedAt);

/// <summary>Remembers the resolved addresses of host names. The caller re-validates every address before it uses it.</summary>
public interface IHostAddressCache
{
    Task<ResolvedHost?> Find(string host, CancellationToken ct);

    Task Save(ResolvedHost resolved, CancellationToken ct);

    /// <summary>Forgets the addresses of a host.</summary>
    Task Clear(string host, CancellationToken ct);
}
