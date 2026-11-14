using System.Net;
using System.Net.Sockets;

namespace SamoBot.Adapters.Http;

/// <summary>
/// The <see cref="SocketsHttpHandler.ConnectCallback"/> of the crawler. It resolves the host of every connection,
/// drops the addresses that the policy denies and connects to the first allowed address that works.
/// Redirect hops open their own connection, so each hop is checked. The callback only supplies the stream:
/// the request URL, the Host header and the TLS server name stay those of the original request.
/// </summary>
public sealed class SafeConnect(SafeResolver resolver, ISocketConnector connector)
{
    public ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken ct) =>
        Connect(context.DnsEndPoint, ct);

    public async ValueTask<Stream> Connect(DnsEndPoint endpoint, CancellationToken ct)
    {
        var addresses = await resolver.ResolveAllowed(endpoint.Host, ct);

        Exception? last = null;
        foreach (var address in addresses)
        {
            try
            {
                return await connector.Connect(address, endpoint.Port, ct);
            }
            catch (SocketException ex)
            {
                last = ex;
            }
        }

        throw last ?? new SocketException((int)SocketError.HostUnreachable);
    }
}
