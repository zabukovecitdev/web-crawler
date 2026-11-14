using System.Net;
using System.Net.Sockets;

namespace SamoBot.Adapters.Http;

/// <summary>
/// The <see cref="SocketsHttpHandler.ConnectCallback"/> of the crawler. It takes the allowed addresses of the host
/// (cached or from DNS), and connects to the first one that works. Redirect hops open their own connection,
/// so each hop is checked. The callback only supplies the stream: the request URL, the Host header and the
/// TLS server name stay those of the original request, although the connection is made by IP address.
/// When every cached address fails, it resolves once more, updates the cache and tries the new addresses.
/// </summary>
public sealed class SafeConnect(SafeResolver resolver, ISocketConnector connector)
{
    public ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken ct) =>
        Connect(context.DnsEndPoint, ct);

    public async ValueTask<Stream> Connect(DnsEndPoint endpoint, CancellationToken ct)
    {
        var resolved = await resolver.ResolveAllowed(endpoint.Host, ct);
        try
        {
            return await ConnectFirst(resolved.Addresses, endpoint.Port, ct);
        }
        catch (SocketException) when (resolved.FromCache)
        {
            var fresh = await resolver.Refresh(endpoint.Host, ct);
            return await ConnectFirst(fresh, endpoint.Port, ct);
        }
    }

    private async ValueTask<Stream> ConnectFirst(IReadOnlyList<IPAddress> addresses, int port, CancellationToken ct)
    {
        Exception? last = null;
        foreach (var address in addresses)
        {
            try
            {
                return await connector.Connect(address, port, ct);
            }
            catch (SocketException ex)
            {
                last = ex;
            }
        }

        throw last ?? new SocketException((int)SocketError.HostUnreachable);
    }
}
