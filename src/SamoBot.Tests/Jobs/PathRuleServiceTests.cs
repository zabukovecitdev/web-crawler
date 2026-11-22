using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Jobs;

public class PathRuleServiceTests
{
    private readonly FakeCrawlJobRepository _jobs = new();
    private readonly FakePathRuleApplier _applier = new(new PathRuleEffect(12, 3));
    private readonly FakeEventPublisher _events = new();
    private readonly PathRuleService _service;

    public PathRuleServiceTests() => _service = new PathRuleService(_jobs, _applier, _events, new FakeTimeProvider(TestData.Now));

    private void AddRunning()
    {
        var job = new CrawlJob { Id = 5, SeedUrls = ["https://a.example/start"], AllowedHosts = ["a.example"] };
        job.Start(TestData.Now);
        _jobs.Add(job);
    }

    [Fact]
    public async Task NewRulesAreSavedAppliedToTheQueueAndAnnounced()
    {
        AddRunning();

        var result = await _service.Change(5, [new PathRule(PathRuleKind.Exclude, "/events/calendar/")], default);

        result.Value.Should().Be(new PathRuleEffect(12, 3));
        _jobs.SavedRules.Should().ContainSingle().Which.PathRules.Should().Contain(new PathRule(PathRuleKind.Exclude, "/events/calendar/"));
        _applier.Applied.Should().Equal(5);
        _events.Published.Should().ContainSingle(e => e.EventType == CrawlEventTypes.JobPathRulesChanged)
            .Which.Payload.Should().BeEquivalentTo(new { stopped = 12, resumed = 3 });
    }

    [Fact]
    public async Task AnUnknownJobIsNotFound()
    {
        var result = await _service.Change(404, [], default);

        result.Errors.Should().ContainSingle().Which.Should().BeOfType<JobNotFoundError>();
    }

    [Fact]
    public async Task AMalformedRuleIsRefusedAndNothingIsApplied()
    {
        AddRunning();

        var result = await _service.Change(5, [new PathRule(PathRuleKind.Exclude, "events")], default);

        result.Errors.Should().ContainSingle().Which.Should().BeOfType<InvalidRequestError>();
        _applier.Applied.Should().BeEmpty();
        _jobs.SavedRules.Should().BeEmpty();
    }

    [Fact]
    public async Task AFinishedJobCannotChangeItsFolders()
    {
        var job = new CrawlJob { Id = 5, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"] };
        job.Complete(TestData.Now);
        _jobs.Add(job);

        var result = await _service.Change(5, [], default);

        result.IsFailed.Should().BeTrue();
        _applier.Applied.Should().BeEmpty();
    }
}
