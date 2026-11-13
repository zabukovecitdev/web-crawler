using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Parsing;

namespace SamoBot.Tests.Parsing;

public class LinkResolverTests
{
    private static readonly Uri Page = new("https://a.example/x");

    private static IReadOnlyList<string> Resolve(params string[] hrefs) =>
        LinkResolver.Resolve(Page, hrefs.Select(href => new ParsedLink { Url = href }));

    [Fact]
    public void ResolveRootRelativeLinkUsesPageHost()
    {
        Resolve("/about").Should().Equal("https://a.example/about");
    }

    [Fact]
    public void ResolveRelativeAndAbsoluteLinksAreResolvedAgainstThePage()
    {
        Resolve("y", "https://b.example/z").Should().Equal("https://a.example/y", "https://b.example/z");
    }

    [Theory]
    [InlineData("mailto:someone@a.example")]
    [InlineData("javascript:void(0)")]
    [InlineData("tel:+4912345")]
    [InlineData("ftp://a.example/file")]
    [InlineData("")]
    public void ResolveNonWebLinksAreRemoved(string href)
    {
        Resolve(href).Should().BeEmpty();
    }

    [Fact]
    public void ResolveNormalizesLinks()
    {
        Resolve("/p?b=2&a=1&utm_source=x#frag").Should().Equal("https://a.example/p?a=1&b=2");
    }

    [Fact]
    public void ResolveRemovesDuplicatesAfterNormalization()
    {
        Resolve("/p#one", "/p#two", "https://A.example/p").Should().Equal("https://a.example/p");
    }
}
