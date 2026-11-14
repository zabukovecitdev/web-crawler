using System.Net;
using System.Net.Sockets;

namespace SamoBot.Adapters.Http;

/// <summary>Opens a TCP connection to one address. A small seam so that tests need no network.</summary>
public interface ISocketConnector
{
    ValueTask<Stream> Connect(IPAddress address, int port, CancellationToken ct);
}

public sealed class TcpSocketConnector : ISocketConnector
{
    public async ValueTask<Stream> Connect(IPAddress address, int port, CancellationToken ct)
    {
        var socket = new Socket(address.AddressFamily, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(new IPEndPoint(address, port), ct);
            return new NetworkStream(socket, ownsSocket: true);
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }
}
