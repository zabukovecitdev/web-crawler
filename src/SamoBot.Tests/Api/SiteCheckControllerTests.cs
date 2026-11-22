using FluentAssertions;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Api.Contracts;
using SamoBot.Api.Controllers;
using SamoBot.Core.Browser;
using SamoBot.Core.Domain;
using SamoBot.Core.SiteChecks;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Api;

public class SiteCheckControllerTests
{
    private readonly FakeTimeProvider _time = new(TestData.Now);

    [Fact]
    public async Task TheBrowserEndpointsReportTheCachedAndTheFreshAnswer()
    {
        var probe = new FakeBrowserProbe();
        var controller = new BrowserController(new BrowserAvailability(probe, _time));
        await controller.Get();
        probe.Available = false;

        var cached = await controller.Get();
        var fresh = await controller.Check();

        cached.Available.Should().BeTrue();
        (fresh.Available, fresh.Reason).Should().Be((false, "The browser service did not answer."));
    }

    [Fact]
    public async Task ThePlanSitemapEndpointQueuesAWalkAndReportsPending()
    {
        var store = new FakeSitemapStore();
        var planner = new SitemapPlanner(new FakeSitemapPlanReader(new SitemapPlanStatus(0, 0, null, null, 0)), store, _time);

        var result = await new HostPlanController(planner).Sitemap("A.Example");

        result.Should().BeOfType<OkObjectResult>().Which.Value.Should().BeOfType<SitemapPlanDto>().Which.State.Should().Be(SitemapPlanState.Pending);
        store.Enqueued.Should().Equal("a.example");
    }

    [Fact]
    public async Task ThePlanSitemapEndpointRefusesABadHost()
    {
        var planner = new SitemapPlanner(new FakeSitemapPlanReader(new SitemapPlanStatus(0, 0, null, null, 0)), new FakeSitemapStore(), _time);

        (await new HostPlanController(planner).Sitemap("a b/c")).Should().BeOfType<BadRequestObjectResult>();
    }
}
