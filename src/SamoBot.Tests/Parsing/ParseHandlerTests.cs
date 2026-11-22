using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Parsing;

public class ParseHandlerTests
{
    private static readonly FetchToParse Fetch = TestDirectives.Fetch("https://a.example/x", depth: 0) with { MaxDepth = 5 };

    private static ParsedPage Page(params MetaTag[] meta) => new()
    {
        Title = "T",
        Links = [new ParsedLink { Url = "/about" }, new ParsedLink { Url = "https://other.example/" }],
        MetaTags = meta
    };

    private static (FakeParseBacklog Queue, ParsePageHandler Handler) Build(ParsedPage page, FetchToParse? fetch = null)
    {
        var store = new FakeContentStore();
        store.Put("page-1", "<html></html>");
        var queue = new FakeParseBacklog(fetch ?? Fetch);
        var handler = new ParsePageHandler(
            store, new FakePageParser(page), queue, TestDirectives.Contexts, TestDirectives.Policies, TestDirectives.Links);
        return (queue, handler);
    }

    private static async Task<CompletedParse> Run(ParsedPage page, FetchToParse? fetch = null)
    {
        var (queue, handler) = Build(page, fetch);
        await handler.Handle(fetch ?? Fetch, default);
        return queue.Completed.Should().ContainSingle().Subject;
    }

    [Fact]
    public async Task HandleResolvesAndFiltersLinksThenCompletes()
    {
        var completed = await Run(Page());

        completed.Page.Title.Should().Be("T");
        completed.Links.Should().Equal(new NewUrl(7, "https://a.example/about", "https://a.example/about", "a.example", 1));
        completed.Policy.FollowLinks.Should().BeTrue();
        completed.Policy.Index.Should().BeTrue();
    }

    [Theory]
    [InlineData("robots", "nofollow")]
    [InlineData("robots", "none")]
    [InlineData("ROBOTS", "NoIndex, NoFollow")]
    [InlineData("samobot", "nofollow")]
    public async Task APageLevelNoFollowFollowsNoLinksButStoresThemAll(string name, string content)
    {
        var completed = await Run(Page(new MetaTag(name, content)));

        completed.Links.Should().BeEmpty();
        completed.PageLinks.Select(l => l.ToUrl).Should().Equal("https://a.example/about", "https://other.example/");
    }

    [Theory]
    [InlineData("googlebot", "nofollow")]
    [InlineData("robots", "noindex")]
    [InlineData("robots", "nonexistent-directive")]
    public async Task AMetaTagThatDoesNotForbidFollowingFollowsTheLinks(string name, string content)
    {
        var completed = await Run(Page(new MetaTag(name, content)));

        completed.Links.Select(l => l.Url).Should().Equal("https://a.example/about");
    }

    [Theory]
    [InlineData("nofollow")]
    [InlineData("samobot: nofollow")]
    [InlineData("googlebot: noindex, samobot: none")]
    public async Task AnXRobotsTagNoFollowFollowsNoLinks(string header)
    {
        var fetch = Fetch with { ResponseHeaders = TestDirectives.Header("X-Robots-Tag", header) };

        var completed = await Run(Page(), fetch);

        completed.Links.Should().BeEmpty();
        completed.PageLinks.Should().HaveCount(2);
    }

    [Fact]
    public async Task AnXRobotsTagForAnotherCrawlerIsIgnored()
    {
        var fetch = Fetch with { ResponseHeaders = TestDirectives.Header("X-Robots-Tag", "googlebot: nofollow") };

        var completed = await Run(Page(), fetch);

        completed.Links.Select(l => l.Url).Should().Equal("https://a.example/about");
    }

    [Fact]
    public async Task LinksWithANoFollowRelValueAreStoredButNotQueued()
    {
        var page = new ParsedPage
        {
            Links =
            [
                new ParsedLink { Url = "/about" },
                new ParsedLink { Url = "/login", Rel = "nofollow" },
                new ParsedLink { Url = "/ad", Rel = "noopener SPONSORED" },
                new ParsedLink { Url = "/comment", Rel = "ugc" },
                new ParsedLink { Url = "/next", Rel = "next noopener" }
            ]
        };

        var completed = await Run(page);

        completed.Links.Select(l => l.Url).Should().Equal("https://a.example/about", "https://a.example/next");
        completed.PageLinks.Where(l => l.NoFollow).Select(l => l.ToUrl)
            .Should().Equal("https://a.example/login", "https://a.example/ad", "https://a.example/comment");
    }

    [Fact]
    public async Task ATargetLinkedBothWithAndWithoutNoFollowIsFollowed()
    {
        var page = new ParsedPage { Links = [new ParsedLink { Url = "/about", Rel = "nofollow" }, new ParsedLink { Url = "/about" }] };

        var completed = await Run(page);

        completed.Links.Select(l => l.Url).Should().Equal("https://a.example/about");
    }

    [Fact]
    public async Task RelativeLinksResolveAgainstTheBaseHref()
    {
        var page = new ParsedPage { BaseHref = "/docs/", Links = [new ParsedLink { Url = "guide.html" }, new ParsedLink { Url = "/top" }] };

        var completed = await Run(page);

        completed.Links.Select(l => l.Url).Should().Equal("https://a.example/docs/guide.html", "https://a.example/top");
        completed.PageLinks.Select(l => l.ToUrl).Should().Equal("https://a.example/docs/guide.html", "https://a.example/top");
    }

    [Theory]
    [InlineData("javascript:alert(1)")]
    [InlineData("")]
    public async Task ABaseHrefThatIsNotHttpIsIgnored(string baseHref)
    {
        var page = new ParsedPage { BaseHref = baseHref, Links = [new ParsedLink { Url = "guide.html" }] };

        var completed = await Run(page);

        completed.Links.Select(l => l.Url).Should().Equal("https://a.example/guide.html");
    }

    [Fact]
    public async Task AMetaRefreshIsQueuedLikeARedirectEvenWhenLinksAreNotFollowed()
    {
        var page = new ParsedPage
        {
            MetaTags = [new MetaTag("refresh", "0; url=/moved", HttpEquiv: true), new MetaTag("robots", "nofollow")],
            Links = [new ParsedLink { Url = "/about" }]
        };

        var completed = await Run(page, Fetch with { Depth = 2 });

        completed.Links.Should().Equal(new NewUrl(7, "https://a.example/moved", "https://a.example/moved", "a.example", 2));
    }

    [Fact]
    public async Task ACanonicalThatNamesAnotherPageIsQueuedAndKeepsThisPageOutOfTheIndex()
    {
        var page = new ParsedPage { Canonical = "/main?utm_source=x", Links = [new ParsedLink { Url = "/main" }] };

        var completed = await Run(page);

        completed.Page.Canonical.Should().Be("https://a.example/main");
        completed.Policy.Index.Should().BeFalse();
        completed.Links.Should().ContainSingle().Which.Should().Be(new NewUrl(7, "https://a.example/main", "https://a.example/main", "a.example", 0));
    }

    [Fact]
    public async Task ACanonicalThatNamesThePageItselfChangesNothing()
    {
        var completed = await Run(new ParsedPage { Canonical = "https://A.example/x#top" });

        completed.Page.Canonical.Should().Be("https://a.example/x");
        completed.Policy.Index.Should().BeTrue();
        completed.Links.Should().BeEmpty();
    }

    [Fact]
    public async Task ACanonicalFromTheLinkHeaderIsUsedWhenTheHtmlNamesNone()
    {
        var fetch = Fetch with { ResponseHeaders = TestDirectives.Header("Link", "</print>; rel=\"alternate\", </main>; rel=\"canonical\"") };

        var completed = await Run(new ParsedPage(), fetch);

        completed.Page.Canonical.Should().Be("https://a.example/main");
        completed.Policy.Index.Should().BeFalse();
    }

    [Fact]
    public async Task TheDirectivesThatAppliedAreKeptForDisplay()
    {
        var fetch = Fetch with { ResponseHeaders = TestDirectives.Header("X-Robots-Tag", "noarchive") };

        var completed = await Run(Page(new MetaTag("robots", "noindex")), fetch);

        completed.Policy.Directives.Should().Equal("noindex (meta robots)", "noarchive (X-Robots-Tag)");
    }

    [Fact]
    public async Task HandleBatchClaimsTenFetchesWithFiveMinuteLease()
    {
        var (queue, pageHandler) = Build(Page());

        var count = await new ParseBatchHandler(queue, pageHandler).HandleBatch(default);

        count.Should().Be(1);
        queue.LastClaim.Should().Be((10, TimeSpan.FromMinutes(5)));
        queue.Completed.Should().HaveCount(1);
    }

    private static readonly PageShell EmptyShell = new(84, 1, 4, 0, true, false);

    [Fact]
    public async Task AnEmptyAppShellInAnAutoJobIsSentBackToBeRenderedInTheBrowser()
    {
        var fetch = Fetch with { CanEscalate = true };
        var (queue, handler) = Build(new ParsedPage { Shell = EmptyShell }, fetch);

        await handler.Handle(fetch, default);

        queue.Escalated.Should().Equal(fetch);
        queue.Completed.Should().BeEmpty();
    }

    [Fact]
    public async Task APageThatWasAlreadyRenderedIsParsedEvenWhenItLooksEmpty()
    {
        var (queue, handler) = Build(new ParsedPage { Shell = EmptyShell });

        await handler.Handle(Fetch, default);

        queue.Escalated.Should().BeEmpty();
        queue.Completed.Should().HaveCount(1);
    }

    [Fact]
    public async Task APageWithItsContentIsNotEscalated()
    {
        var fetch = Fetch with { CanEscalate = true };
        var (queue, handler) = Build(new ParsedPage { Shell = new PageShell(5000, 100, 2, 0, false, false) }, fetch);

        await handler.Handle(fetch, default);

        queue.Escalated.Should().BeEmpty();
        queue.Completed.Should().HaveCount(1);
    }

    [Fact]
    public async Task LinksOutsideTheTickedFoldersAreNotQueued()
    {
        var fetch = Fetch with { PathRules = [new PathRule(PathRuleKind.Exclude, "/about/")] };

        var completed = await Run(Page(), fetch);

        completed.Links.Should().BeEmpty();
        completed.PageLinks.Should().HaveCount(2);
    }
}
