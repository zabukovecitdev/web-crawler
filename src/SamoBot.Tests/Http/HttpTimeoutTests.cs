using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using FluentAssertions;
using Microsoft.Extensions.Options;
using SamoBot.Adapters.Browser;
using SamoBot.Adapters.Http;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Http;

/// <summary>The deadlines against a real socket on loopback: a server that never answers, and one that drips its body.</summary>
public class HttpTimeoutTests
{
    private static readonly TimeSpan Drip = TimeSpan.FromMilliseconds(200);

    private static CrawlerNetworkOptions Limits(int requestSeconds = 1) => new()
    {
        FirstByteTimeoutSeconds = 1,
        RequestTimeoutSeconds = requestSeconds,
        RobotsTimeoutSeconds = 1,
        SitemapTimeoutSeconds = 1
    };

    private static HttpClient Client(Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>? connect = null)
    {
        var handler = new SocketsHttpHandler { AutomaticDecompression = DecompressionMethods.None };
        if (connect is not null)
        {
            handler.ConnectCallback = connect;
        }

        return new HttpClient(handler) { Timeout = Timeout.InfiniteTimeSpan };
    }

    private static HttpFetcher Fetcher(CrawlerNetworkOptions? limits = null, HttpClient? client = null) =>
        new(client ?? Client(), Options.Create(new ChromeRenderingOptions()), Options.Create(limits ?? Limits()), TimeProvider.System);

    private static HttpRobotsSource Robots() => new(Client(), Options.Create(Limits()), TimeProvider.System);

    private static HttpSitemapSource Sitemap() => new(Client(), Options.Create(Limits()), TimeProvider.System);

    private static ClaimedUrl Url(LoopbackServer server) => TestData.Url($"http://{server.Authority}/page");

    private sealed class HangingConnector : ISocketConnector
    {
        public async ValueTask<Stream> Connect(IPAddress address, int port, CancellationToken ct)
        {
            await Task.Delay(Timeout.Infinite, ct);
            throw new SocketException((int)SocketError.TimedOut);
        }
    }

    private sealed class OneAddressDns : IDnsResolver
    {
        public Task<IReadOnlyList<IPAddress>> Resolve(string host, CancellationToken ct) =>
            Task.FromResult<IReadOnlyList<IPAddress>>([IPAddress.Parse("93.184.216.34")]);
    }

    [Fact]
    public async Task AServerThatNeverAnswersEndsAsATimeoutAtTheFirstByteDeadline()
    {
        await using var server = LoopbackServer.Silent();
        var clock = Stopwatch.StartNew();

        var response = await Fetcher().Fetch(Url(server), default);

        (response.StatusCode, response.Error).Should().Be((0, "timeout"));
        response.ResponseTimeMs.Should().BeInRange(900, 3000);
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task AConnectThatNeverFinishesEndsAsATimeoutAtTheConnectDeadline()
    {
        var limits = new CrawlerNetworkOptions { ConnectTimeoutSeconds = 1, FirstByteTimeoutSeconds = 30, RequestTimeoutSeconds = 30 };
        var resolver = new SafeResolver(new OneAddressDns(), new FakeHostAddressCache(), Options.Create(limits), TimeProvider.System);
        var connect = new SafeConnect(resolver, new HangingConnector(), Options.Create(limits));
        var clock = Stopwatch.StartNew();

        var response = await Fetcher(limits, Client(connect.ConnectAsync)).Fetch(TestData.Url("http://a.example/x"), default);

        (response.StatusCode, response.Error).Should().Be((0, "timeout"));
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(5));
    }

    [Fact]
    public async Task ASlowDripBodyEndsAsATimeoutAtTheTotalDeadline()
    {
        await using var server = LoopbackServer.SlowDrip(Drip);
        var clock = Stopwatch.StartNew();

        var response = await Fetcher().Fetch(Url(server), default);

        (response.StatusCode, response.Error).Should().Be((0, "timeout"));
        response.ResponseTimeMs.Should().BeInRange(900, 3000);
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task AFastResponseIsNotAffected()
    {
        await using var server = LoopbackServer.Fast("<html>ok</html>");

        var response = await Fetcher().Fetch(Url(server), default);

        response.StatusCode.Should().Be(200);
        response.Error.Should().BeNull();
        System.Text.Encoding.UTF8.GetString(response.Content!).Should().Be("<html>ok</html>");
    }

    [Fact]
    public async Task AShutdownDuringTheBodyReadStillThrows()
    {
        await using var server = LoopbackServer.SlowDrip(Drip);
        using var shutdown = new CancellationTokenSource(TimeSpan.FromMilliseconds(400));

        var act = () => Fetcher(Limits(requestSeconds: 60)).Fetch(Url(server), shutdown.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }

    [Fact]
    public async Task RobotsFromAServerThatNeverAnswersBlocksTheHost()
    {
        await using var server = LoopbackServer.Silent();
        var clock = Stopwatch.StartNew();

        var rules = await Robots().Download(server.Authority, default);

        rules.IsAllowed("/", "SamoBot").Should().BeFalse();
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(6));
    }

    [Fact]
    public async Task RobotsWithASlowDripBodyBlocksTheHostAtTheDeadline()
    {
        await using var server = LoopbackServer.SlowDrip(Drip);
        var clock = Stopwatch.StartNew();

        var rules = await Robots().Download(server.Authority, default);

        rules.IsAllowed("/", "SamoBot").Should().BeFalse();
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(6));
    }

    [Fact]
    public async Task ARobotsFileFromAFastServerIsRead()
    {
        await using var server = LoopbackServer.Fast("User-agent: *\nDisallow: /private\n", "text/plain");

        var rules = await Robots().Download(server.Authority, default);

        rules.IsAllowed("/private", "SamoBot").Should().BeFalse();
        rules.IsAllowed("/open", "SamoBot").Should().BeTrue();
    }

    [Fact]
    public async Task ASitemapFromAServerThatNeverAnswersIsAFailure()
    {
        await using var server = LoopbackServer.Silent();
        var clock = Stopwatch.StartNew();

        var download = await Sitemap().Download($"http://{server.Authority}/s.xml", default);

        download.StatusCode.Should().Be(0);
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task ASitemapWithASlowDripBodyIsAFailureAtTheDeadline()
    {
        await using var server = LoopbackServer.SlowDrip(Drip);
        var clock = Stopwatch.StartNew();

        var download = await Sitemap().Download($"http://{server.Authority}/s.xml", default);

        download.StatusCode.Should().Be(0);
        download.Content.Should().BeNull();
        clock.Elapsed.Should().BeLessThan(TimeSpan.FromSeconds(4));
    }

    [Fact]
    public async Task ASitemapFromAFastServerIsRead()
    {
        await using var server = LoopbackServer.Fast("<urlset/>", "application/xml");

        var download = await Sitemap().Download($"http://{server.Authority}/s.xml", default);

        download.IsSuccess.Should().BeTrue();
        download.Content.Should().HaveCount(9);
    }

    [Fact]
    public async Task XRobotsTagHeadersFromTheWireAreKeptOnePerLine()
    {
        await using var server = LoopbackServer.Fast(
            "<html>ok</html>", extraHeaders: "X-Robots-Tag: noindex\r\nX-Robots-Tag: samobot: noarchive, nofollow\r\n");

        var response = await Fetcher().Fetch(Url(server), default);

        response.XRobotsTag.Should().Be("noindex\nsamobot: noarchive, nofollow");
    }
}
