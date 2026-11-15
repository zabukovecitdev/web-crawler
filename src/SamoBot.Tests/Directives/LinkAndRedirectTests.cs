using FluentAssertions;
using SamoBot.Core.Directives.Canonical;
using SamoBot.Core.Directives.Links;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Directives;

public class LinkAndRedirectTests
{
    [Theory]
    [InlineData("", true)]
    [InlineData("nofollow", false)]
    [InlineData("NoFollow", false)]
    [InlineData("noopener nofollow", false)]
    [InlineData("sponsored", false)]
    [InlineData("ugc", false)]
    [InlineData("nofollowing", true)]
    [InlineData("next", true)]
    public void TheRelRuleBlocksOnlyWholeConfiguredValues(string rel, bool allowed) =>
        new RelLinkRule("nofollow", "sponsored", "ugc").Allows(new ParsedLink { Url = "/a", Rel = rel }, TestDirectives.Context(new ParsedPage()))
            .Should().Be(allowed);

    [Fact]
    public void WhichRelValuesBlockIsChosenAtRegistration() =>
        new RelLinkRule("nofollow").Allows(new ParsedLink { Url = "/a", Rel = "sponsored" }, TestDirectives.Context(new ParsedPage()))
            .Should().BeTrue();

    [Theory]
    [InlineData("0; url=https://b.example/x", "https://b.example/x")]
    [InlineData("5;URL='/next'", "/next")]
    [InlineData("0, url=\"/q\"", "/q")]
    [InlineData("3; /plain", "/plain")]
    [InlineData("  0 ;  url = /spaced ", "/spaced")]
    [InlineData("1.5; url=/decimal", "/decimal")]
    [InlineData("30", null)]
    [InlineData("0; url=", null)]
    [InlineData("soon; url=/x", null)]
    [InlineData("", null)]
    public void MetaRefreshReadsTheTarget(string content, string? expected) => MetaRefreshSource.Target(content).Should().Be(expected);

    [Fact]
    public void MetaRefreshTargetsResolveAgainstTheBaseUrl()
    {
        var page = new ParsedPage { BaseHref = "https://a.example/docs/", MetaTags = [new MetaTag("Refresh", "0; url=next", HttpEquiv: true)] };

        new MetaRefreshSource().Targets(TestDirectives.Context(page)).Should().Equal(new Uri("https://a.example/docs/next"));
    }

    [Fact]
    public void AMetaRefreshNamedTagIsNotAnHttpEquiv() =>
        new MetaRefreshSource().Targets(TestDirectives.Context(new ParsedPage { MetaTags = [new MetaTag("refresh", "0; url=/x")] }))
            .Should().BeEmpty();

    [Theory]
    [InlineData("</main>; rel=\"canonical\"", "https://a.example/main")]
    [InlineData("<https://b.example/p>; rel=canonical", "https://b.example/p")]
    [InlineData("</a>; rel=\"alternate\", </main>; rel=\"preload canonical\"", "https://a.example/main")]
    [InlineData("</a>; rel=\"alternate\"", null)]
    [InlineData("<javascript:x>; rel=canonical", null)]
    public void TheLinkHeaderNamesACanonical(string header, string? expected)
    {
        var page = TestDirectives.Context(new ParsedPage { BaseHref = "https://cdn.example/" }, headers: TestDirectives.Header("Link", header));

        new LinkHeaderCanonicalSource().Find(page)?.AbsoluteUri.Should().Be(expected);
        if (expected is null)
        {
            new LinkHeaderCanonicalSource().Find(page).Should().BeNull();
        }
    }

    [Fact]
    public void TheHtmlCanonicalWinsOverTheLinkHeader()
    {
        var page = new ParsedPage { Canonical = "/from-html" };

        var context = TestDirectives.Context(page, headers: TestDirectives.Header("Link", "</from-header>; rel=canonical"));

        context.Canonical!.AbsoluteUri.Should().Be("https://a.example/from-html");
    }

    [Fact]
    public void TheHtmlCanonicalResolvesAgainstTheBaseHref() =>
        new HtmlCanonicalSource().Find(TestDirectives.Context(new ParsedPage { BaseHref = "/docs/", Canonical = "main" }))!
            .AbsoluteUri.Should().Be("https://a.example/docs/main");
}
