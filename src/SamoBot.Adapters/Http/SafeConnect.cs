using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Http;

/// <summary>
/// The <see cref="SocketsHttpHandler.ConnectCallback"/> of the crawler. It takes the allowed addresses of the host
/// (cached or from DNS), and connects to the first one that works. Redirect hops open their own connection,
/// so each hop is checked. The callback only supplies the stream: the request URL, the Host header and the
/// TLS server name stay those of the original request, although the connection is made by IP address.
/// DNS and connect (the cache refresh included) must finish within <c>Crawler:ConnectTimeoutSeconds</c>, else the callback
/// throws a <see cref="TimeoutException"/>. TLS runs after the callback and is covered by the first-byte deadline.
/// When every cached address fails, it resolves once more, updates the cache and tries the new addresses.
/// </summary>
public sealed class SafeConnect(SafeResolver resolver, ISocketConnector connector, IOptions<CrawlerNetworkOptions> options)
{
    public ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken ct) =>
        Connect(context.DnsEndPoint, ct);

    public async ValueTask<Stream> Connect(DnsEndPoint endpoint, CancellationToken ct)
    {
        using var limit = CancellationTokenSource.CreateLinkedTokenSource(ct);
        limit.CancelAfter(RequestDeadline.Seconds(options.Value.ConnectTimeoutSeconds));
        try
        {
            return await ConnectWithin(endpoint, limit.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw new TimeoutException($"Connect to {endpoint.Host}:{endpoint.Port} timed out.");
        }
    }

    private async ValueTask<Stream> ConnectWithin(DnsEndPoint endpoint, CancellationToken ct)
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
