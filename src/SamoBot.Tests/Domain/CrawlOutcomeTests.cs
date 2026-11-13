using FluentAssertions;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Domain;

public class CrawlOutcomeTests
{
    private static readonly DateTimeOffset Now = new(2026, 1, 1, 12, 0, 0, TimeSpan.Zero);

    private static ClaimedUrl Url(int failCount = 2) => new()
    {
        Id = 1,
        CrawlJobId = 7,
        Host = "a.example",
        Url = "https://a.example/x",
        NormalizedUrl = "https://a.example/x",
        Depth = 3,
        FailCount = failCount,
        UseJsRendering = false,
        RespectRobots = true
    };

    [Fact]
    public void SucceededSetsIdleResetsFailCountAndSchedulesRecrawl()
    {
        var update = new Succeeded(TimeSpan.FromDays(1)).ToUpdate(Url(), Now);

        update.Should().Be(new UrlUpdate(UrlStatus.Idle, Now.AddDays(1), 0, null, null));
    }

    [Fact]
    public void RetryLaterIncrementsFailCountAndAppliesDelay()
    {
        var update = new RetryLater("http-500", TimeSpan.FromMinutes(4)).ToUpdate(Url(), Now);

        update.Should().Be(new UrlUpdate(UrlStatus.Idle, Now.AddMinutes(4), 3, "http-500", null));
    }

    [Fact]
    public void DisabledKeepsFailCountAndStoresReason()
    {
        var update = new Disabled("robots").ToUpdate(Url(), Now);

        update.Should().Be(new UrlUpdate(UrlStatus.Disabled, null, 2, "robots", null));
    }

    [Fact]
    public void RedirectedDisablesUrlAndAddsTargetAtSameDepth()
    {
        var update = new Redirected("/new?b=2&a=1").ToUpdate(Url(), Now);

        update.Status.Should().Be(UrlStatus.Disabled);
        update.FailReason.Should().Be("redirect");
        update.NewUrl.Should().Be(new NewUrl(7, "https://a.example/new?b=2&a=1", "https://a.example/new?a=1&b=2", "a.example", 3));
    }

    [Fact]
    public void RedirectedToNonWebSchemeAddsNoNewUrl()
    {
        var update = new Redirected("ftp://a.example/file").ToUpdate(Url(), Now);

        update.NewUrl.Should().BeNull();
    }
}
