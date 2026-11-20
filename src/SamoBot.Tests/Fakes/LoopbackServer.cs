using System.Net;
using System.Net.Sockets;
using System.Text;

namespace SamoBot.Tests.Fakes;

public sealed class LoopbackServer : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _loop;

    private LoopbackServer(Func<NetworkStream, CancellationToken, Task> onConnection)
    {
        _listener.Start();
        _loop = Accept(onConnection);
    }

    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    public string Authority => $"127.0.0.1:{Port}";

    public static LoopbackServer Silent() => new((_, ct) => Task.Delay(Timeout.Infinite, ct));

    public static LoopbackServer SlowDrip(TimeSpan interval) => new(async (stream, ct) =>
    {
        await Write(stream, "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\nContent-Length: 100000\r\n\r\n", ct);
        while (true)
        {
            await Write(stream, "x", ct);
            await Task.Delay(interval, ct);
        }
    });

    public static LoopbackServer Fast(string body, string contentType = "text/html", string status = "200 OK", string extraHeaders = "") =>
        new(async (stream, ct) =>
        {
            var buffer = new byte[4096];
            _ = await stream.ReadAsync(buffer, ct);
            var length = Encoding.UTF8.GetByteCount(body);
            await Write(
                stream,
                $"HTTP/1.1 {status}\r\nContent-Type: {contentType}\r\nContent-Length: {length}\r\n{extraHeaders}Connection: close\r\n\r\n{body}",
                ct);
        });

    public async ValueTask DisposeAsync()
    {
        await _stop.CancelAsync();
        _listener.Stop();
        await _loop;
        _stop.Dispose();
    }

    private static async Task Write(NetworkStream stream, string text, CancellationToken ct) =>
        await stream.WriteAsync(Encoding.ASCII.GetBytes(text), ct);

    private async Task Accept(Func<NetworkStream, CancellationToken, Task> onConnection)
    {
        while (!_stop.IsCancellationRequested && await NextClient() is { } client)
        {
            _ = Serve(client, onConnection);
        }
    }

    private async Task<TcpClient?> NextClient()
    {
        try
        {
            return await _listener.AcceptTcpClientAsync(_stop.Token);
        }
        catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException or SocketException)
        {
            return null;
        }
    }

    private async Task Serve(TcpClient client, Func<NetworkStream, CancellationToken, Task> onConnection)
    {
        using (client)
        {
            await onConnection(client.GetStream(), _stop.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }
}
