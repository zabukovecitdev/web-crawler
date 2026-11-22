using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;
using SamoBot.Core.Browser;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

public class CrawlJobsControllerTests
{
    private readonly FakeCrawlJobRepository _jobs = new();
    private readonly FakeUrlFrontier _frontier = new();
    private readonly FakeBrowserProbe _browser = new();
    private readonly CrawlJobsController _controller;

    public CrawlJobsControllerTests()
    {
        var service = new CrawlJobService(_jobs, _frontier, new FakeEventPublisher(), Gate(_browser), new FakeSitemapSeeder(), new FakeTimeProvider(TestData.Now));
        _controller = new CrawlJobsController(service, _jobs, new FakeJobLinkReader())
        {
            ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() }
        };
    }

    private static BrowserGate Gate(FakeBrowserProbe probe) => new(new BrowserAvailability(probe, new FakeTimeProvider(TestData.Now)));

    private void AddJob(CrawlJobStatus status = CrawlJobStatus.Pending)
    {
        var job = new CrawlJob { Id = 5, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"] };
        if (status is CrawlJobStatus.Running)
        {
            job.Start(TestData.Now);
        }

        _jobs.Add(job);
    }

    private static CrawlJobActionRequest Action(string action) => new() { Action = action };

    [Fact]
    public async Task ActionReturns204WhenTheTransitionWorks()
    {
        AddJob();

        (await _controller.Actions(5, Action("start"))).Should().BeOfType<NoContentResult>();
    }

    [Fact]
    public async Task ActionIsCaseInsensitive()
    {
        AddJob();

        (await _controller.Actions(5, Action("START"))).Should().BeOfType<NoContentResult>();
    }

    [Fact]
    public async Task ActionReturns404ForAMissingJob()
    {
        var result = await _controller.Actions(99, Action("start"));

        result.Should().BeOfType<ObjectResult>().Which.StatusCode.Should().Be(StatusCodes.Status404NotFound);
    }

    [Fact]
    public async Task ActionReturns409ForAnInvalidTransition()
    {
        AddJob();

        var result = await _controller.Actions(5, Action("pause"));

        result.Should().BeOfType<ObjectResult>().Which.StatusCode.Should().Be(StatusCodes.Status409Conflict);
    }

    [Fact]
    public async Task ReprocessActionOfARunningJobReturns409()
    {
        AddJob(CrawlJobStatus.Running);

        var result = await _controller.Actions(5, Action("reprocess"));

        result.Should().BeOfType<ObjectResult>().Which.StatusCode.Should().Be(StatusCodes.Status409Conflict);
    }

    [Theory]
    [InlineData("explode")]
    [InlineData("")]
    public async Task ActionReturns400ForAnUnknownAction(string action) =>
        (await _controller.Actions(5, Action(action))).Should().BeOfType<BadRequestObjectResult>();

    [Fact]
    public async Task CreateReturns400WithMessagesForUnsafeSeeds()
    {
        var request = new CreateCrawlJobRequest { SeedUrls = ["http://localhost/", "https://ok.example/"] };

        var result = await _controller.Create(request);

        var problem = result.Should().BeOfType<BadRequestObjectResult>().Which.Value.Should().BeOfType<ValidationProblemDetails>().Subject;
        problem.Errors["seedUrls"].Should().ContainSingle().Which.Should().StartWith("Seed URL 1:");
    }

    [Fact]
    public async Task CreateReturns201WithTheDtoAndDoesNotStartTheJobByDefault()
    {
        var result = await _controller.Create(new CreateCrawlJobRequest { SeedUrls = ["https://a.example/"] });

        var created = result.Should().BeOfType<CreatedAtActionResult>().Subject;
        created.Value.Should().BeOfType<CrawlJobDto>().Which.Status.Should().Be("Pending");
        _frontier.AddedSeeds.Should().BeEmpty();
    }

    [Fact]
    public async Task CreateWithStartImmediatelyStartsTheJob()
    {
        var request = new CreateCrawlJobRequest { SeedUrls = ["https://a.example/"], StartImmediately = true };

        var result = await _controller.Create(request);

        var dto = result.Should().BeOfType<CreatedAtActionResult>().Which.Value.Should().BeOfType<CrawlJobDto>().Subject;
        dto.Status.Should().Be("Running");
        _frontier.AddedSeeds.Should().ContainSingle();
    }

    [Fact]
    public async Task RecrawlActionQueuesEveryLink()
    {
        AddJob(CrawlJobStatus.Running);

        var result = await _controller.Actions(5, Action("recrawl"), default);

        result.Should().BeOfType<NoContentResult>();
        _frontier.Requeued.Should().ContainSingle().Which.Scope.Should().Be(RequeueScope.All);
    }

    [Fact]
    public async Task RecrawlOfALinkThatCannotBeQueuedReturns409()
    {
        AddJob(CrawlJobStatus.Running);
        _frontier.RequeueResult = 0;

        var result = await _controller.Recrawl(5, 77, default);

        result.Should().BeOfType<ObjectResult>().Which.StatusCode.Should().Be(StatusCodes.Status409Conflict);
        _frontier.Requeued.Should().ContainSingle().Which.Scope.Should().Be(RequeueScope.Link(77));
    }
}
