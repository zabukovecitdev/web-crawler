using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Domain;
using SamoBot.Core.SiteChecks;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.SiteChecks;

public class SitemapPlannerTests
{
    [Fact]
    public async Task ASitemapThatIsNeverRequestedIsQueuedForTheWorkerAndReportedAsPending()
    {
        var store = new FakeSitemapStore();
        var planner = new SitemapPlanner(new FakeSitemapPlanReader(new SitemapPlanStatus(0, 0, null, null, 0)), store, new FakeTimeProvider(TestData.Now));

        var plan = await planner.Ensure("a.example", default);

        plan.State.Should().Be(SitemapPlanState.Pending);
        store.Enqueued.Should().Equal("a.example");
    }

    [Fact]
    public async Task AFreshSitemapIsReturnedAsStoredAndNothingIsQueued()
    {
        var store = new FakeSitemapStore();
        var done = TestData.Now.AddDays(-1);
        var planner = new SitemapPlanner(new FakeSitemapPlanReader(new SitemapPlanStatus(3, 1893, done, done, 0)), store, new FakeTimeProvider(TestData.Now));

        var plan = await planner.Ensure("a.example", default);

        plan.Should().Be(new SitemapPlan(SitemapPlanState.Ready, 3, 1893, done));
        store.Enqueued.Should().BeEmpty();
    }
}
