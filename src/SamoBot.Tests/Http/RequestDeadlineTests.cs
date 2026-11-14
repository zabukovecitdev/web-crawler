using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Browser;
using SamoBot.Adapters.Http;

namespace SamoBot.Tests.Http;

public class RequestDeadlineTests
{
    private static readonly TimeSpan Total = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan FirstByte = TimeSpan.FromSeconds(10);

    private readonly FakeTimeProvider _time = new();

    [Fact]
    public void TheTotalDeadlineCancelsTheToken()
    {
        using var deadline = new RequestDeadline(Total, FirstByte, _time, default);
        deadline.HeadersReceived();

        _time.Advance(Total - TimeSpan.FromMilliseconds(1));
        deadline.Token.IsCancellationRequested.Should().BeFalse();
        _time.Advance(TimeSpan.FromMilliseconds(1));

        deadline.Token.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public void TheFirstByteDeadlineCancelsTheTokenBeforeTheHeadersArrive()
    {
        using var deadline = new RequestDeadline(Total, FirstByte, _time, default);

        _time.Advance(FirstByte);

        deadline.Token.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public void TheHeadersSwitchTheFirstByteDeadlineOff()
    {
        using var deadline = new RequestDeadline(Total, FirstByte, _time, default);

        deadline.HeadersReceived();
        _time.Advance(FirstByte + TimeSpan.FromSeconds(1));

        deadline.Token.IsCancellationRequested.Should().BeFalse();
    }

    [Fact]
    public void TheCallerTokenCancelsTheDeadlineToken()
    {
        using var source = new CancellationTokenSource();
        using var deadline = new RequestDeadline(Total, FirstByte, _time, source.Token);

        source.Cancel();

        deadline.Token.IsCancellationRequested.Should().BeTrue();
    }

    [Fact]
    public void ADurationOfZeroTurnsADeadlineOff()
    {
        using var deadline = new RequestDeadline(TimeSpan.Zero, TimeSpan.Zero, _time, default);

        _time.Advance(TimeSpan.FromDays(1));

        deadline.Token.IsCancellationRequested.Should().BeFalse();
    }

    [Fact]
    public void ADeadlineThatFiresAfterDisposeDoesNotThrow()
    {
        var deadline = new RequestDeadline(Total, FirstByte, _time, default);
        deadline.Dispose();

        var act = () => _time.Advance(Total);

        act.Should().NotThrow();
    }

    [Fact]
    public void TheDefaultsAreThePolicyOfTheCrawler()
    {
        var network = new CrawlerNetworkOptions();
        var chrome = new ChromeRenderingOptions();

        (network.ConnectTimeoutSeconds, network.FirstByteTimeoutSeconds, network.RequestTimeoutSeconds,
                network.RobotsTimeoutSeconds, network.SitemapTimeoutSeconds)
            .Should().Be((5, 10, 20, 10, 30));
        chrome.DefaultTimeoutMs.Should().Be(30_000);
        chrome.TotalTimeoutMs.Should().BeGreaterThan(chrome.DefaultTimeoutMs);
    }
}
