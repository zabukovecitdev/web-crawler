using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.SiteChecks;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.SiteChecks;

public class StartPageResolverTests
{
    private static StartPageResolver Resolver(ScriptedFetcher fetcher, string robots = "User-agent: *\nDisallow:\n")
    {
        var time = new FakeTimeProvider(TestData.Now);
        var rules = new RobotsRules("any", robots, TestData.Now, TestData.Now.AddDays(1));
        var gate = new RobotsGate(new MemoryCache(Options.Create(new MemoryCacheOptions())), new FakeRobotsStore(), new FakeRobotsSource(rules), time);
        return new StartPageResolver(new FetcherResolver([fetcher]), gate);
    }

    [Fact]
    public async Task APageThatAnswersIsReturnedAsIs()
    {
        var fetcher = new ScriptedFetcher(false, _ => ScriptedFetcher.Html("<p>hi</p>"));

        var page = await Resolver(fetcher).Resolve(new Uri("https://a.example/guide"), default);

        (page.Reachable, page.Redirected, page.Url.AbsoluteUri).Should().Be((true, false, "https://a.example/guide"));
    }

    [Fact]
    public async Task ARedirectToAnotherHostIsFollowedAndTheCrawlUsesTheFinalHost()
    {
        var fetcher = new ScriptedFetcher(false, url => url.StartsWith("https://example.com", StringComparison.Ordinal)
            ? ScriptedFetcher.Redirect("https://docs.example.com/guide/install")
            : ScriptedFetcher.Html());

        var page = await Resolver(fetcher).Resolve(new Uri("https://example.com/"), default);

        (page.Url.Authority, page.Redirected, page.Reachable).Should().Be(("docs.example.com", true, true));
        fetcher.Requested.Should().Equal("https://example.com/", "https://docs.example.com/guide/install");
    }

    [Fact]
    public async Task ARelativeRedirectIsResolvedAgainstTheCurrentUrl()
    {
        var fetcher = new ScriptedFetcher(false, url => url.EndsWith("/old", StringComparison.Ordinal) ? ScriptedFetcher.Redirect("/new") : ScriptedFetcher.Html());

        var page = await Resolver(fetcher).Resolve(new Uri("https://a.example/old"), default);

        page.Url.AbsoluteUri.Should().Be("https://a.example/new");
    }

    [Fact]
    public async Task EndlessRedirectsStopAfterTheHopLimit()
    {
        var fetcher = new ScriptedFetcher(false, url => ScriptedFetcher.Redirect(url + "x"));

        var page = await Resolver(fetcher).Resolve(new Uri("https://a.example/"), default);

        page.Problem.Should().Be("The address redirects too many times.");
        fetcher.Requested.Should().HaveCount(StartPageResolver.MaxHops);
    }

    [Fact]
    public async Task ARedirectToALocalAddressIsNotFollowed()
    {
        var fetcher = new ScriptedFetcher(false, _ => ScriptedFetcher.Redirect("http://localhost/admin"));

        var page = await Resolver(fetcher).Resolve(new Uri("https://a.example/"), default);

        page.Url.Authority.Should().Be("a.example");
        page.Reachable.Should().BeFalse();
        fetcher.Requested.Should().HaveCount(1);
    }

    [Fact]
    public async Task RobotsTxtStopsTheCheckBeforeThePageIsRequested()
    {
        var fetcher = new ScriptedFetcher(false, _ => ScriptedFetcher.Html());

        var page = await Resolver(fetcher, "User-agent: *\nDisallow: /private/\n").Resolve(new Uri("https://a.example/private/x"), default);

        (page.BlockedByRobots, page.Reachable).Should().Be((true, false));
        fetcher.Requested.Should().BeEmpty();
    }

    [Fact]
    public async Task ANetworkErrorIsReportedAsAProblem()
    {
        var fetcher = new ScriptedFetcher(false, _ => new FetchResponse { StatusCode = 0, Error = "connection refused" });

        var page = await Resolver(fetcher).Resolve(new Uri("https://a.example/"), default);

        (page.Reachable, page.Problem).Should().Be((false, "connection refused"));
    }

    [Fact]
    public async Task ABlockedAddressIsReportedAsAProblem()
    {
        var fetcher = new ScriptedFetcher(false, _ => FetchResponse.BlockedAddress());

        var page = await Resolver(fetcher).Resolve(new Uri("https://a.example/"), default);

        page.Problem.Should().Be("The address is local, private or reserved.");
    }

    [Fact]
    public async Task APageThatAnswers404IsNotReachable()
    {
        var fetcher = new ScriptedFetcher(false, _ => ScriptedFetcher.Html(status: 404));

        var page = await Resolver(fetcher).Resolve(new Uri("https://a.example/missing"), default);

        (page.Reachable, page.StatusCode).Should().Be((false, 404));
    }
}
