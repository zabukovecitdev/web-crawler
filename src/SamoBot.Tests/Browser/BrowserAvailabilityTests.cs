using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Browser;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Browser;

public class BrowserAvailabilityTests
{
    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeBrowserProbe _probe = new();

    [Fact]
    public async Task CurrentProbesOnceAndKeepsTheAnswerForAShortWhile()
    {
        var availability = new BrowserAvailability(_probe, _time);

        await availability.Current(default);
        _time.Advance(TimeSpan.FromSeconds(4));
        _probe.Available = false;
        var cached = await availability.Current(default);

        cached.Available.Should().BeTrue();
        _probe.Probes.Should().Be(1);
    }

    [Fact]
    public async Task CurrentProbesAgainWhenTheAnswerIsOld()
    {
        var availability = new BrowserAvailability(_probe, _time);
        await availability.Current(default);
        _time.Advance(BrowserAvailability.MaxAge);
        _probe.Available = false;

        var status = await availability.Current(default);

        status.Available.Should().BeFalse();
        status.Reason.Should().Be("The browser service did not answer.");
        status.CheckedAt.Should().Be(_time.GetUtcNow());
    }

    [Fact]
    public async Task RefreshAlwaysProbes()
    {
        var availability = new BrowserAvailability(_probe, _time);
        await availability.Refresh(default);
        _probe.Available = false;

        (await availability.Refresh(default)).Available.Should().BeFalse();
        (await availability.Current(default)).Available.Should().BeFalse();
        _probe.Probes.Should().Be(2);
    }
}
