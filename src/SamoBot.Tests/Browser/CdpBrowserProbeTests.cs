using System.Net;
using FluentAssertions;
using Microsoft.Extensions.Options;
using SamoBot.Adapters.Browser;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Browser;

public class CdpBrowserProbeTests
{
    private static CdpBrowserProbe Probe(Func<HttpRequestMessage, HttpResponseMessage> respond, bool enabled = true, StubHttpHandler? handler = null) =>
        new(
            new HttpClient(handler ?? new StubHttpHandler(respond)),
            Options.Create(new ChromeRenderingOptions { Enabled = enabled, CdpEndpoint = enabled ? "http://172.31.251.10:9222/" : null }));

    [Fact]
    public async Task ARunningBrowserAnswersTheVersionEndpoint()
    {
        var handler = new StubHttpHandler(_ => new HttpResponseMessage(HttpStatusCode.OK));

        var result = await Probe(_ => throw new InvalidOperationException(), handler: handler).Probe(default);

        result.Available.Should().BeTrue();
        handler.Requests.Should().ContainSingle().Which.RequestUri!.ToString().Should().Be("http://172.31.251.10:9222/json/version");
    }

    [Fact]
    public async Task AnErrorStatusMeansTheServiceIsDown()
    {
        var result = await Probe(_ => new HttpResponseMessage(HttpStatusCode.BadGateway)).Probe(default);

        result.Should().BeEquivalentTo(new { Available = false, Reason = "The browser service answered with status 502." });
    }

    [Fact]
    public async Task ANetworkErrorMeansTheServiceIsDown()
    {
        var result = await Probe(_ => throw new HttpRequestException("refused")).Probe(default);

        result.Should().BeEquivalentTo(new { Available = false, Reason = "The browser service did not answer." });
    }

    [Fact]
    public async Task RenderingThatIsNotConfiguredIsNeverProbed()
    {
        var handler = new StubHttpHandler(_ => new HttpResponseMessage(HttpStatusCode.OK));

        var result = await Probe(_ => throw new InvalidOperationException(), enabled: false, handler: handler).Probe(default);

        result.Available.Should().BeFalse();
        handler.Requests.Should().BeEmpty();
    }

    [Fact]
    public async Task ACancellationByTheCallerStillPropagates()
    {
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        var act = () => Probe(_ => throw new TaskCanceledException()).Probe(cts.Token);

        await act.Should().ThrowAsync<OperationCanceledException>();
    }
}
