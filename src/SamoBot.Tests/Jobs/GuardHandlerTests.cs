using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Browser;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Jobs;

public class GuardHandlerTests
{
    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeEventPublisher _events = new();
    private readonly FakeBrowserPauseStore _store = new() { Paused = [3, 4], Resumed = [5] };

    private BrowserGuardHandler Guard(bool browserUp) =>
        new(new BrowserAvailability(new FakeBrowserProbe(browserUp), _time), _store, _events, _time);

    [Fact]
    public async Task WhenTheBrowserIsDownTheJobsThatNeedItArePausedWithAReason()
    {
        await Guard(false).HandleBatch(default);

        _store.ResumeCalls.Should().Be(0);
        _events.Published.Select(e => (e.JobId, e.EventType)).Should().Equal((3, CrawlEventTypes.JobPaused), (4, CrawlEventTypes.JobPaused));
        _events.Published[0].Payload.Should().BeEquivalentTo(new { reason = PauseReasons.BrowserUnavailable });
    }

    [Fact]
    public async Task WhenTheBrowserIsBackTheJobsPausedForItGoOn()
    {
        await Guard(true).HandleBatch(default);

        _store.PauseCalls.Should().Be(0);
        _events.Published.Should().ContainSingle().Which.Should().Match<(int JobId, string EventType, object Payload)>(e => e.JobId == 5 && e.EventType == CrawlEventTypes.JobStarted);
    }

    [Fact]
    public async Task TheReviewGuardAnnouncesEachJobThatReachedTheReviewPoint()
    {
        var count = await new ReviewGuardHandler(new FakeReviewPauseStore([8]), _events, _time).HandleBatch(default);

        count.Should().Be(0);
        _events.Published.Should().ContainSingle().Which.Payload.Should().BeEquivalentTo(new { reason = PauseReasons.Review });
    }
}
