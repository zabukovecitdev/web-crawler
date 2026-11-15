using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;

namespace SamoBot.Tests.Parsing;

public class PageLinkPreparerTests
{
    private static readonly Uri Page = new("https://a.example/x");

    private static IReadOnlyList<PageLink> Prepare(params ParsedLink[] links) =>
        PageLinkPreparer.Prepare(Page, Page, links, link => link.Rel == "nofollow");

    private static ParsedLink Link(string url, string text = "", bool noFollow = false) =>
        new() { Url = url, LinkText = text, Rel = noFollow ? "nofollow" : string.Empty };

    [Fact]
    public void LinksUseTheSameNormalizationAsTheFrontier()
    {
        var hrefs = new[] { "/a?b=2&a=1&utm_source=x#frag", "HTTPS://B.example:443/Z" };

        var prepared = Prepare(hrefs.Select(h => Link(h)).ToArray());

        prepared.Select(l => l.ToUrl).Should().Equal(LinkResolver.Resolve(Page, hrefs.Select(h => Link(h))));
        prepared.Select(l => l.ToUrl).Should().Equal("https://a.example/a?a=1&b=2", "https://b.example/Z");
    }

    [Theory]
    [InlineData("mailto:someone@a.example")]
    [InlineData("javascript:void(0)")]
    [InlineData("data:text/plain,hi")]
    [InlineData("tel:+4912345")]
    [InlineData("#top")]
    [InlineData("  #top")]
    [InlineData("")]
    public void LinksThatAreNotHttpOrAreFragmentOnlyAreSkipped(string href)
    {
        Prepare(Link(href)).Should().BeEmpty();
    }

    [Fact]
    public void ADuplicateTargetKeepsTheFirstAnchor()
    {
        var prepared = Prepare(Link("/a", "first"), Link("/a#section", "second", noFollow: true), Link("/b", "other"));

        prepared.Should().HaveCount(2);
        prepared[0].Should().Be(new PageLink("https://a.example/a", "a.example", "first", true, false));
    }

    [Fact]
    public void AnchorTextIsTrimmedCollapsedAndTruncated()
    {
        var prepared = Prepare(Link("/a", "  Hello \n\t  world  "), Link("/b", new string('x', 500)), Link("/c", "   "));

        prepared[0].AnchorText.Should().Be("Hello world");
        prepared[1].AnchorText.Should().HaveLength(PageLinkPreparer.MaxAnchorLength);
        prepared[2].AnchorText.Should().BeNull();
    }

    [Fact]
    public void InternalMeansTheSameAuthorityAsThePage()
    {
        var prepared = Prepare(Link("/a"), Link("https://A.example/b"), Link("https://sub.a.example/c"), Link("http://a.example/d"), Link("https://a.example:8443/e"));

        prepared.Select(l => l.IsInternal).Should().Equal(true, true, false, true, false);
        prepared.Select(l => l.ToHost).Should().Equal("a.example", "a.example", "sub.a.example", "a.example", "a.example:8443");
    }

    [Fact]
    public void NoFollowIsWhatTheGivenRuleSays()
    {
        Prepare(Link("/a", noFollow: true), Link("/b")).Select(l => l.NoFollow).Should().Equal(true, false);
    }

    [Fact]
    public void TargetsResolveAgainstTheBaseUrlButInternalMeansTheAuthorityOfThePage()
    {
        var prepared = PageLinkPreparer.Prepare(new Uri("https://cdn.example/docs/"), Page, [Link("guide")], _ => false);

        prepared.Single().Should().Be(new PageLink("https://cdn.example/docs/guide", "cdn.example", null, false, false));
    }

    [Fact]
    public void ATargetLongerThanTheLimitIsSkipped()
    {
        var prepared = Prepare(Link("/" + new string('a', PageLinkPreparer.MaxUrlLength)), Link("/ok"));

        prepared.Should().ContainSingle().Which.ToUrl.Should().Be("https://a.example/ok");
    }

    [Fact]
    public void AtMostTheCapIsKept()
    {
        var links = Enumerable.Range(0, PageLinkPreparer.MaxLinksPerPage + 50).Select(i => Link($"/p{i}")).ToArray();

        var prepared = Prepare(links);

        prepared.Should().HaveCount(PageLinkPreparer.MaxLinksPerPage);
        prepared[^1].ToUrl.Should().Be($"https://a.example/p{PageLinkPreparer.MaxLinksPerPage - 1}");
    }
}
