using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Core.SiteChecks;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

public class TreeControllerTests
{
    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeCrawlJobRepository _jobs = new();
    private readonly FakePathRuleApplier _applier = new(new PathRuleEffect(2, 0));

    public TreeControllerTests() =>
        _jobs.Add(new CrawlJob { Id = 5, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"] });

    private HostPlanTreeController PlanController(params FolderGroup[] groups)
    {
        var rules = RobotsRules.AllowAll("a.example", TestData.Now, TimeSpan.FromDays(1));
        var gate = new RobotsGate(new MemoryCache(Options.Create(new MemoryCacheOptions())), new FakeRobotsStore(), new FakeRobotsSource(rules), _time);
        return new HostPlanTreeController(new PlanTreeService(new FakeSitemapPlanReader(new SitemapPlanStatus(1, 1, null, null, 0), groups), gate));
    }

    private CrawlJobPathRulesController RulesController() => new(new PathRuleService(_jobs, _applier, new FakeEventPublisher(), _time));

    [Fact]
    public async Task ThePlanTreeIsReturnedAsJson()
    {
        var result = await PlanController(new FolderGroup("guide/", "a", FolderCounts.None with { Pages = 3 })).Tree("a.example");

        var tree = result.Should().BeOfType<OkObjectResult>().Which.Value.Should().BeOfType<PathTreeDto>().Subject;
        tree.Nodes.Single().Should().Match<TreeNodeDto>(node => node.Prefix == "/guide/" && node.Counts.Pages == 3 && node.Children != null);
    }

    [Fact]
    public async Task ThePlanTreeRefusesABadHostAndABadParent()
    {
        (await PlanController().Tree("a b")).Should().BeOfType<BadRequestObjectResult>();
        (await PlanController().Tree("a.example", "guide")).Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task TheJobTreeAnswers404ForAnUnknownJob() =>
        (await new CrawlJobTreeController(new JobTreeService(_jobs, new FakeJobTreeReader([]))).Tree(99)).Should().BeOfType<NotFoundResult>();

    [Fact]
    public async Task TheJobTreeIsReturnedForAKnownJob() =>
        (await new CrawlJobTreeController(new JobTreeService(_jobs, new FakeJobTreeReader([]))).Tree(5)).Should().BeOfType<OkObjectResult>();

    [Fact]
    public async Task PuttingRulesAnswers204AndTheQueueIsUpdated()
    {
        var request = new PathRulesRequest { Rules = [new PathRuleRequest { Kind = "Exclude", Prefix = "/events/calendar/" }] };

        (await RulesController().PutRules(5, request)).Should().BeOfType<NoContentResult>();

        _applier.Applied.Should().Equal(5);
    }

    [Fact]
    public async Task PuttingABadRuleAnswers400()
    {
        var request = new PathRulesRequest { Rules = [new PathRuleRequest { Kind = "Maybe", Prefix = "/x/" }] };

        (await RulesController().PutRules(5, request)).Should().BeOfType<BadRequestObjectResult>();
    }

    [Fact]
    public async Task PuttingRulesOfAnUnknownJobAnswers404() =>
        (await RulesController().PutRules(99, new PathRulesRequest())).Should().BeOfType<ObjectResult>().Which.StatusCode.Should().Be(404);

    [Fact]
    public async Task PuttingRulesOfACompletedJobAnswers409()
    {
        var job = new CrawlJob { Id = 6, SeedUrls = ["https://a.example/"], AllowedHosts = ["a.example"] };
        job.Complete(TestData.Now);
        _jobs.Add(job);

        (await RulesController().PutRules(6, new PathRulesRequest())).Should().BeOfType<ObjectResult>().Which.StatusCode.Should().Be(409);
    }
}
