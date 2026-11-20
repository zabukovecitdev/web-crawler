using System.Net;
using System.Net.Sockets;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Http;

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
