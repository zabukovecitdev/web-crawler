using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Browser;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Core.SiteChecks;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.SiteChecks;

public class SiteCheckServiceTests
{
    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeSitemapStore _sitemapStore = new();
    private readonly FakeBrowserProbe _probe = new();

    private sealed class ConstantParser(PageShell shell) : IPageParser
    {
        public ParsedPage Parse(Stream content, string? contentType) => new() { Shell = shell };
    }

    private SiteCheckService Service(ScriptedFetcher http, PageShell shell, string robots = "User-agent: *\nDisallow: /admin/\nCrawl-delay: 2\n")
    {
        var rules = new RobotsRules("any", robots, TestData.Now, TestData.Now.AddDays(1));
        var gate = new RobotsGate(new MemoryCache(Options.Create(new MemoryCacheOptions())), new FakeRobotsStore(), new FakeRobotsSource(rules), _time);
        var fetchers = new FetcherResolver([http, new ScriptedFetcher(true, _ => ScriptedFetcher.Html())]);
        return new SiteCheckService(
            new StartPageResolver(fetchers, gate),
            gate,
            new SitemapPlanner(new FakeSitemapPlanReader(new SitemapPlanStatus(0, 0, null, null, 0)), _sitemapStore, _time),
            new BrowserAvailability(_probe, _time),
            new JsDetectionService(new ConstantParser(shell), fetchers),
            _time);
    }

    private static ScriptedFetcher Redirecting() =>
        new(false, url => url.Contains("docs", StringComparison.Ordinal) ? ScriptedFetcher.Html() : ScriptedFetcher.Redirect("https://docs.example.com/guide/install"));

    [Fact]
    public async Task TheCheckFollowsTheRedirectAndReadsRobotsAndTheSitemapOfTheFinalHost()
    {
        var result = (await Service(Redirecting(), new PageShell(6214, 143, 2, 0, false, false)).Check("https://example.com/", default)).Value;

        result.Host.Should().Be("docs.example.com");
        result.StartUrl.Should().Be("https://docs.example.com/guide/install");
        result.StartPage.Redirected.Should().BeTrue();
        (result.Robots.StartAllowed, result.Robots.CrawlDelaySeconds).Should().Be((true, 2.0));
        result.Robots.Disallowed.Should().Equal("/admin/");
        result.Sitemap.State.Should().Be(SitemapPlanState.Pending);
        _sitemapStore.Enqueued.Should().Equal("docs.example.com");
        result.JavaScript.Verdict.Should().Be(JsVerdict.NotNeeded);
        result.Browser.Available.Should().BeTrue();
    }

    [Fact]
    public async Task ASiteThatNeedsJavaScriptAndHasNoBrowserIsProbablyNeeded()
    {
        _probe.Available = false;

        var result = (await Service(Redirecting(), new PageShell(84, 1, 4, 0, true, false)).Check("https://example.com/", default)).Value;

        result.JavaScript.Verdict.Should().Be(JsVerdict.ProbablyNeeded);
        result.Browser.Available.Should().BeFalse();
    }

    [Theory]
    [InlineData("")]
    [InlineData("ftp://a.example/")]
    [InlineData("http://localhost/")]
    [InlineData("not a url")]
    public async Task AnInvalidAddressIsRefusedBeforeAnythingIsFetched(string url)
    {
        var fetcher = new ScriptedFetcher(false, _ => ScriptedFetcher.Html());

        var result = await Service(fetcher, PageShell.Empty).Check(url, default);

        result.IsFailed.Should().BeTrue();
        fetcher.Requested.Should().BeEmpty();
    }

    [Fact]
    public async Task ARobotsRuleThatForbidsTheStartPageIsReportedAndNothingIsFetched()
    {
        var fetcher = new ScriptedFetcher(false, _ => ScriptedFetcher.Html());

        var result = (await Service(fetcher, PageShell.Empty).Check("https://a.example/admin/x", default)).Value;

        (result.Robots.StartAllowed, result.StartPage.BlockedByRobots).Should().Be((false, true));
        fetcher.Requested.Should().BeEmpty();
    }
}
