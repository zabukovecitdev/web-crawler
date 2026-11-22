using System.Text.Json;
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

public class CrawlJobsGateControllerTests
{
    private readonly FakeCrawlJobRepository _jobs = new();
    private readonly FakeBrowserProbe _browser = new(available: false);
    private readonly CrawlJobsController _controller;

    public CrawlJobsGateControllerTests()
    {
        var time = new FakeTimeProvider(TestData.Now);
        var gate = new BrowserGate(new BrowserAvailability(_browser, time));
        var service = new CrawlJobService(_jobs, new FakeUrlFrontier(), new FakeEventPublisher(), gate, time);
        _controller = new CrawlJobsController(service, _jobs, new FakeJobLinkReader())
        {
            ControllerContext = new ControllerContext { HttpContext = new DefaultHttpContext() }
        };
    }

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static string Json(object? value) => JsonSerializer.Serialize(value, Web);

    [Fact]
    public async Task CreatingAJobThatNeedsTheBrowserWhileItIsDownAnswers409WithAMachineReadableCode()
    {
        var request = new CreateCrawlJobRequest { SeedUrls = ["https://a.example/"], RenderMode = "Always", StartImmediately = true };

        var result = await _controller.Create(request);

        var objectResult = result.Should().BeOfType<ObjectResult>().Subject;
        objectResult.StatusCode.Should().Be(StatusCodes.Status409Conflict);
        var problem = objectResult.Value.Should().BeOfType<ProblemDetails>().Subject;
        problem.Extensions["code"].Should().Be("browser-unavailable");
        problem.Detail.Should().Be("The crawl renders every page in a browser, but the browser service is not available.");
        Json(problem).Should().Contain("\"code\":\"browser-unavailable\"");
    }

    [Fact]
    public async Task TheOldSwitchStillMeansAlways()
    {
        var request = new CreateCrawlJobRequest { SeedUrls = ["https://a.example/"], UseJsRendering = true, StartImmediately = true };

        var result = await _controller.Create(request);

        result.Should().BeOfType<ObjectResult>().Which.StatusCode.Should().Be(StatusCodes.Status409Conflict);
    }

    [Fact]
    public async Task ModeNeverStartsWhileTheBrowserIsDown()
    {
        var request = new CreateCrawlJobRequest { SeedUrls = ["https://a.example/"], RenderMode = "Never", StartImmediately = true };

        var result = await _controller.Create(request);

        result.Should().BeOfType<CreatedAtActionResult>().Which.Value.Should().BeOfType<CrawlJobDto>().Which.RenderMode.Should().Be(RenderMode.Never);
    }

    [Fact]
    public async Task APathRuleWithoutASlashIsAnError()
    {
        var request = new CreateCrawlJobRequest
        {
            SeedUrls = ["https://a.example/"],
            PathRules = [new PathRuleRequest { Kind = "Exclude", Prefix = "blog" }]
        };

        var result = await _controller.Create(request);

        result.Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task StartingAPausedJobWhileTheBrowserIsStillDownAnswers409WithTheCode()
    {
        _jobs.Add(new CrawlJob { Id = 5, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"], RenderMode = RenderMode.Always });

        var result = await _controller.Actions(5, new CrawlJobActionRequest { Action = "start" });

        result.Should().BeOfType<ObjectResult>().Which.Value.Should().BeOfType<ProblemDetails>().Which.Extensions["code"].Should().Be("browser-unavailable");
    }
}
