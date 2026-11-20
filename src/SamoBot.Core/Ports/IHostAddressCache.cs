using System.Net;

namespace SamoBot.Core.Ports;

public sealed record ResolvedHost(string Host, IReadOnlyList<IPAddress> Addresses, DateTimeOffset ResolvedAt);

public interface IHostAddressCache
{
    Task<ResolvedHost?> Find(string host, CancellationToken ct);

    Task Save(ResolvedHost resolved, CancellationToken ct);

    Task Clear(string host, CancellationToken ct);
}
