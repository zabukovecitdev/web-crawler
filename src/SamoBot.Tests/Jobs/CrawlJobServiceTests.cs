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
    public async Task CreateInsertsPendingJobAndPublishesEvent()
    {
        var result = await _service.Create(Request("https://A.example/start", "https://b.example/"), default);

        result.IsSuccess.Should().BeTrue();
        result.Value.Status.Should().Be(CrawlJobStatus.Pending);
        result.Value.AllowedHosts.Should().Equal("a.example", "b.example");
        _events.Published.Should().ContainSingle(e => e.JobId == 42 && e.EventType == CrawlEventTypes.JobCreated);
    }

    [Fact]
    public async Task CreateWithoutValidSeedFailsAndInsertsNothing()
    {
        var result = await _service.Create(Request("", "not a url"), default);

        result.IsFailed.Should().BeTrue();
        _events.Published.Should().BeEmpty();
    }

    [Fact]
    public async Task StartAddsNormalizedSeedsSavesRunningJobAndPublishes()
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
    public async Task StartOfUnknownJobFails()
    {
        (await _service.Start(99, default)).IsFailed.Should().BeTrue();
    }

    [Fact]
    public async Task StartOfRunningJobFailsWithoutSideEffects()
    {
        AddJob(CrawlJobStatus.Running);

        var result = await _service.Start(5, default);

        result.IsFailed.Should().BeTrue();
        _frontier.AddedSeeds.Should().BeEmpty();
        _jobs.Saved.Should().BeEmpty();
        _events.Published.Should().BeEmpty();
    }

    [Fact]
    public async Task PauseOfRunningJobSavesAndPublishes()
    {
        var job = AddJob(CrawlJobStatus.Running);

        var result = await _service.Pause(5, default);

        result.IsSuccess.Should().BeTrue();
        job.Status.Should().Be(CrawlJobStatus.Paused);
        _events.Published.Should().ContainSingle(e => e.EventType == CrawlEventTypes.JobPaused);
    }

    [Fact]
    public async Task PauseOfPendingJobFailsWithoutSaving()
    {
        AddJob();

        (await _service.Pause(5, default)).IsFailed.Should().BeTrue();

        _jobs.Saved.Should().BeEmpty();
        _events.Published.Should().BeEmpty();
    }

    [Fact]
    public async Task CancelSavesAndPublishes()
    {
        var job = AddJob(CrawlJobStatus.Running);

        var result = await _service.Cancel(5, default);

        result.IsSuccess.Should().BeTrue();
        job.Status.Should().Be(CrawlJobStatus.Cancelled);
        _events.Published.Should().ContainSingle(e => e.EventType == CrawlEventTypes.JobCancelled);
    }

    [Fact]
    public async Task CreateKeepsANonDefaultPortInTheAllowedHost()
    {
        var result = await _service.Create(Request("http://example.com:5000/start"), default);

        result.Value.AllowedHosts.Should().Equal("example.com:5000");
    }

    [Theory]
    [InlineData("http://localhost/")]
    [InlineData("http://169.254.169.254/")]
    [InlineData("https://user:pw@a.example/")]
    [InlineData("ftp://a.example/")]
    public async Task CreateRejectsUnsafeSeedsWithoutStoringAnything(string seed)
    {
        var result = await _service.Create(Request("https://ok.example/", seed), default);

        result.IsFailed.Should().BeTrue();
        result.Errors.Should().ContainSingle().Which.Message.Should().StartWith("Seed URL 2:");
        _events.Published.Should().BeEmpty();
        _jobs.Saved.Should().BeEmpty();
    }

    [Fact]
    public async Task CreateAndStartAddsSeedsAndPublishesCreatedThenStarted()
    {
        var result = await _service.CreateAndStart(Request("https://a.example/"), default);

        result.IsSuccess.Should().BeTrue();
        _frontier.AddedSeeds.Should().ContainSingle();
        _events.Published.Select(e => e.EventType).Should().Equal(CrawlEventTypes.JobCreated, CrawlEventTypes.JobStarted);
    }

    [Fact]
    public async Task CreateAndStartWithUnsafeSeedStartsNothing()
    {
        var result = await _service.CreateAndStart(Request("http://10.0.0.1/"), default);

        result.IsFailed.Should().BeTrue();
        _frontier.AddedSeeds.Should().BeEmpty();
    }

    [Fact]
    public async Task ActionsOnAMissingJobFailWithAJobNotFoundError()
    {
        (await _service.Start(99, default)).HasError<JobNotFoundError>().Should().BeTrue();
        (await _service.Pause(99, default)).HasError<JobNotFoundError>().Should().BeTrue();
        (await _service.Cancel(99, default)).HasError<JobNotFoundError>().Should().BeTrue();
    }

    [Fact]
    public async Task AnInvalidTransitionIsNotAJobNotFoundError()
    {
        AddJob(CrawlJobStatus.Pending);

        var result = await _service.Pause(5, default);

        result.IsFailed.Should().BeTrue();
        result.HasError<JobNotFoundError>().Should().BeFalse();
    }
}
