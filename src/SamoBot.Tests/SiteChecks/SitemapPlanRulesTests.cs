using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.SiteChecks;
using SamoBot.Core.Sitemaps;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.SiteChecks;

public class SitemapPlanRulesTests
{
    private static readonly DateTimeOffset Now = TestData.Now;

    [Fact]
    public void AFreshWalkWithFilesIsReady() =>
        SitemapPlanRules.Decide(new SitemapPlanStatus(3, 1893, Now.AddDays(-2), Now.AddDays(-2), 0), Now)
            .Should().Be((SitemapPlanState.Ready, false));

    [Fact]
    public void AFreshWalkWithoutFilesMeansThereIsNoSitemap() =>
        SitemapPlanRules.Decide(new SitemapPlanStatus(0, 0, Now.AddDays(-1), Now.AddDays(-1), 0), Now)
            .Should().Be((SitemapPlanState.None, false));

    [Fact]
    public void AWalkThatWasRequestedAndNotFinishedIsPending() =>
        SitemapPlanRules.Decide(new SitemapPlanStatus(0, 0, Now.AddSeconds(-10), null, 1), Now)
            .Should().Be((SitemapPlanState.Pending, false));

    [Fact]
    public void AWalkThatKeepsFailingIsFailed() =>
        SitemapPlanRules.Decide(new SitemapPlanStatus(0, 0, Now.AddHours(-3), null, SitemapBatchHandler.MaxAttempts), Now)
            .Should().Be((SitemapPlanState.Failed, false));

    [Fact]
    public void ANeverRequestedSitemapIsQueued() =>
        SitemapPlanRules.Decide(new SitemapPlanStatus(0, 0, null, null, 0), Now)
            .Should().Be((SitemapPlanState.Pending, true));

    [Fact]
    public void AStaleWalkIsQueuedAgain() =>
        SitemapPlanRules.Decide(new SitemapPlanStatus(3, 100, Now.AddDays(-9), Now.AddDays(-8), 0), Now)
            .Should().Be((SitemapPlanState.Pending, true));
}
