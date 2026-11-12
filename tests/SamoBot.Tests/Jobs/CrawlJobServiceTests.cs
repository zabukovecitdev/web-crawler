using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Jobs;

public class CrawlJobServiceTests
{
    private readonly FakeCrawlJobRepository _jobs = new();
    private readonly FakeUrlFrontier _frontier = new();
    private readonly FakeEventPublisher _events = new();
    private readonly CrawlJobService _service;

    public CrawlJobServiceTests()
    {
        _service = new CrawlJobService(_jobs, _frontier, _events, new FakeTimeProvider(TestData.Now));
    }

    private static NewCrawlJob Request(params string[] seeds) => new(null, seeds, 2, 100, false, true);

    private CrawlJob AddJob(CrawlJobStatus status = CrawlJobStatus.Pending)
    {
        var job = new CrawlJob { Id = 5, SeedUrls = ["https://a.example/start", "not a url"], AllowedHosts = ["a.example"] };
        if (status is CrawlJobStatus.Running)
        {
            job.Start(TestData.Now);
        }

        _jobs.Add(job);
        return job;
    }

    [Fact]
    public async Task Create_InsertsPendingJobAndPublishesEvent()
    {
        var result = await _service.Create(Request("https://A.example/start", "https://b.example/"), default);

        result.IsSuccess.Should().BeTrue();
        result.Value.Status.Should().Be(CrawlJobStatus.Pending);
        result.Value.AllowedHosts.Should().Equal("a.example", "b.example");
        _events.Published.Should().ContainSingle(e => e.JobId == 42 && e.EventType == CrawlEventTypes.JobCreated);
    }

    [Fact]
    public async Task Create_WithoutValidSeed_FailsAndInsertsNothing()
    {
        var result = await _service.Create(Request("", "not a url"), default);

        result.IsFailed.Should().BeTrue();
        _events.Published.Should().BeEmpty();
    }

    [Fact]
    public async Task Start_AddsNormalizedSeedsSavesRunningJobAndPublishes()
    {
        var job = AddJob();

        var result = await _service.Start(5, default);

        result.IsSuccess.Should().BeTrue();
        job.Status.Should().Be(CrawlJobStatus.Running);
        _frontier.AddedSeeds.Should().ContainSingle().Which.Seeds.Should()
            .Equal(new NewUrl(5, "https://a.example/start", "https://a.example/start", "a.example", 0));
        _jobs.Saved.Should().Equal(job);
        _events.Published.Should().ContainSingle(e => e.EventType == CrawlEventTypes.JobStarted);
    }

    [Fact]
    public async Task Start_OfUnknownJob_Fails()
    {
        (await _service.Start(99, default)).IsFailed.Should().BeTrue();
    }

    [Fact]
    public async Task Start_OfRunningJob_FailsWithoutSideEffects()
    {
        AddJob(CrawlJobStatus.Running);

        var result = await _service.Start(5, default);

        result.IsFailed.Should().BeTrue();
        _frontier.AddedSeeds.Should().BeEmpty();
        _jobs.Saved.Should().BeEmpty();
        _events.Published.Should().BeEmpty();
    }

    [Fact]
    public async Task Pause_OfRunningJob_SavesAndPublishes()
    {
        var job = AddJob(CrawlJobStatus.Running);

        var result = await _service.Pause(5, default);

        result.IsSuccess.Should().BeTrue();
        job.Status.Should().Be(CrawlJobStatus.Paused);
        _events.Published.Should().ContainSingle(e => e.EventType == CrawlEventTypes.JobPaused);
    }

    [Fact]
    public async Task Pause_OfPendingJob_FailsWithoutSaving()
    {
        AddJob();

        (await _service.Pause(5, default)).IsFailed.Should().BeTrue();

        _jobs.Saved.Should().BeEmpty();
        _events.Published.Should().BeEmpty();
    }

    [Fact]
    public async Task Cancel_SavesAndPublishes()
    {
        var job = AddJob(CrawlJobStatus.Running);

        var result = await _service.Cancel(5, default);

        result.IsSuccess.Should().BeTrue();
        job.Status.Should().Be(CrawlJobStatus.Cancelled);
        _events.Published.Should().ContainSingle(e => e.EventType == CrawlEventTypes.JobCancelled);
    }
}
