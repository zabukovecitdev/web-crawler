using System.Net;
using System.Net.Sockets;
using System.Text;
using FluentAssertions;
using Microsoft.Extensions.Options;
using SamoBot.Adapters.Http;
using SamoBot.Core.Common;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Http;

public class SafeConnectTests
{
    private sealed class FakeDns(Dictionary<string, IPAddress[]> map) : IDnsResolver
    {
        public Task<IReadOnlyList<IPAddress>> Resolve(string host, CancellationToken ct) =>
            map.TryGetValue(host, out var found)
                ? Task.FromResult<IReadOnlyList<IPAddress>>(found)
                : throw new SocketException((int)SocketError.HostNotFound);
    }

    private sealed class FakeConnector(params string[] failing) : ISocketConnector
    {
        public List<IPAddress> Attempts { get; } = [];

        public ValueTask<Stream> Connect(IPAddress address, int port, CancellationToken ct)
        {
            Attempts.Add(address);
            return failing.Contains(address.ToString())
                ? throw new SocketException((int)SocketError.ConnectionRefused)
                : ValueTask.FromResult<Stream>(new MemoryStream());
        }
    }

    private static SafeConnect Build(IDnsResolver dns, ISocketConnector connector, bool allowPrivate = false) =>
        new(new SafeResolver(dns, new FakeHostAddressCache(), Options.Create(new CrawlerNetworkOptions { AllowPrivateAddresses = allowPrivate }), TimeProvider.System), connector);

    private static DnsEndPoint Context(string host, int port = 443) => new(host, port);

    private static FakeDns Dns(string host, params string[] addresses) =>
        new(new() { [host] = addresses.Select(IPAddress.Parse).ToArray() });

    [Fact]
    public async Task ConnectToAPublicAddressSucceeds()
    {
        var connector = new FakeConnector();
        var connect = Build(Dns("a.example", "93.184.216.34"), connector);

        await connect.Connect(Context("a.example"), default);

        connector.Attempts.Should().Equal(IPAddress.Parse("93.184.216.34"));
    }

    [Fact]
    public async Task ConnectWhenEveryAddressIsBlockedThrowsAndNeverConnects()
    {
        var connector = new FakeConnector();
        var connect = Build(Dns("evil.example", "10.0.0.5", "169.254.169.254"), connector);

        var act = async () => await connect.Connect(Context("evil.example"), default);

        (await act.Should().ThrowAsync<BlockedAddressException>()).Which.Host.Should().Be("evil.example");
        connector.Attempts.Should().BeEmpty();
    }

    [Fact]
    public async Task ConnectWithMixedAddressesSkipsTheBlockedOnes()
    {
        var connector = new FakeConnector();
        var connect = Build(Dns("mixed.example", "127.0.0.1", "8.8.8.8", "192.168.0.1"), connector);

        await connect.Connect(Context("mixed.example"), default);

        connector.Attempts.Should().Equal(IPAddress.Parse("8.8.8.8"));
    }

    [Fact]
    public async Task ConnectTriesTheNextAllowedAddressWhenOneFails()
    {
        var connector = new FakeConnector("8.8.8.8");
        var connect = Build(Dns("m.example", "8.8.8.8", "10.0.0.1", "1.1.1.1"), connector);

        await connect.Connect(Context("m.example"), default);

        connector.Attempts.Should().Equal(IPAddress.Parse("8.8.8.8"), IPAddress.Parse("1.1.1.1"));
    }

    [Fact]
    public async Task ConnectWhenAllAllowedAddressesFailThrowsTheSocketError()
    {
        var connect = Build(Dns("m.example", "8.8.8.8"), new FakeConnector("8.8.8.8"));

        var act = async () => await connect.Connect(Context("m.example"), default);

        await act.Should().ThrowAsync<SocketException>();
    }

    [Theory]
    [InlineData("127.0.0.1")]
    [InlineData("169.254.169.254")]
    [InlineData("[::1]")]
    [InlineData("::ffff:10.0.0.1")]
    public async Task ConnectToABlockedIpLiteralThrowsWithoutResolvingDns(string host)
    {
        var connector = new FakeConnector();
        var connect = Build(new FakeDns([]), connector);

        var act = async () => await connect.Connect(Context(host), default);

        await act.Should().ThrowAsync<BlockedAddressException>();
        connector.Attempts.Should().BeEmpty();
    }

    [Fact]
    public async Task ConnectToAPublicIpLiteralSucceeds()
    {
        var connector = new FakeConnector();

        await Build(new FakeDns([]), connector).Connect(Context("8.8.8.8"), default);

        connector.Attempts.Should().Equal(IPAddress.Parse("8.8.8.8"));
    }

    [Fact]
    public async Task ConnectWithPrivateAddressesAllowedConnectsToLoopback()
    {
        var connector = new FakeConnector();

        await Build(Dns("local.test", "127.0.0.1"), connector, allowPrivate: true).Connect(Context("local.test"), default);

        connector.Attempts.Should().Equal(IPAddress.Loopback);
    }

    [Fact]
    public async Task ConnectByIpKeepsTheHostNameInTheHostHeader()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var requestText = ServeOnce(listener);
        var connect = Build(Dns("site.test", "127.0.0.1"), new TcpSocketConnector(), allowPrivate: true);
        using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false, ConnectCallback = connect.ConnectAsync });

        using var response = await client.GetAsync($"http://site.test:{port}/path?q=1");

        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var text = await requestText;
        text.Should().StartWith("GET /path?q=1 HTTP/1.1");
        text.Should().Contain($"Host: site.test:{port}");
    }

    [Fact]
    public async Task HttpClientSurfacesABlockedHostAsAnHttpRequestExceptionWithTheBlockedCause()
    {
        var connect = Build(Dns("evil.example", "10.0.0.5"), new FakeConnector());
        using var client = new HttpClient(new SocketsHttpHandler { UseProxy = false, ConnectCallback = connect.ConnectAsync });

        var act = async () => await client.GetAsync("http://evil.example/");

        var thrown = (await act.Should().ThrowAsync<HttpRequestException>()).Which;
        BlockedAddressException.IsCause(thrown).Should().BeTrue();
    }

    private static async Task<string> ServeOnce(TcpListener listener)
    {
        using var tcp = await listener.AcceptTcpClientAsync();
        var stream = tcp.GetStream();
        var buffer = new byte[4096];
        var read = await stream.ReadAsync(buffer);
        var request = Encoding.ASCII.GetString(buffer, 0, read);
        await stream.WriteAsync(Encoding.ASCII.GetBytes("HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok"));
        return request;
    }
}
