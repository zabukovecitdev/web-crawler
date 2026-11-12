using System.Text;
using FluentAssertions;
using SamoBot.Adapters.Html;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Html;

public class HtmlAgilityPackParserTests
{
    private const string Page = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
          <title> Hello &amp; welcome </title>
          <meta name="description" content="A page">
          <meta name="keywords" content="a, b">
          <meta name="author" content="Ann">
          <meta name="robots" content="noindex, nofollow">
          <link rel="canonical" href="https://a.example/canonical">
          <meta property="og:title" content="OG title">
          <meta name="twitter:card" content="summary">
          <script type="application/ld+json">{"@type":"Thing"}</script>
          <style>body { color: red; }</style>
        </head>
        <body>
          <h1>Main   heading</h1>
          <h2>Sub</h2>
          <p>Some   text <script>var hidden = 1;</script>here.</p>
          <a href="/about" rel="nofollow" title="About us">About  us</a>
          <a href="">empty</a>
          <img src="/logo.png" alt="Logo" width="10" height="20">
          <img alt="no source">
        </body>
        </html>
        """;

    private static ParsedPage Parse(string html) => new HtmlAgilityPackParser().Parse(new MemoryStream(Encoding.UTF8.GetBytes(html)));

    [Fact]
    public void Parse_ExtractsMetadata()
    {
        var page = Parse(Page);

        page.Title.Should().Be("Hello & welcome");
        page.Description.Should().Be("A page");
        page.Keywords.Should().Be("a, b");
        page.Author.Should().Be("Ann");
        page.Language.Should().Be("en");
        page.Canonical.Should().Be("https://a.example/canonical");
    }

    [Fact]
    public void Parse_ExtractsTextWithoutScriptsAndStyles()
    {
        var page = Parse(Page);

        page.Headings.Should().Equal(new ParsedHeading(1, "Main heading"), new ParsedHeading(2, "Sub"));
        page.BodyText.Should().Contain("Some text here.").And.NotContain("hidden").And.NotContain("color: red");
    }

    [Fact]
    public void Parse_ExtractsLinksAndImagesAndSkipsEntriesWithoutTarget()
    {
        var page = Parse(Page);

        page.Links.Should().ContainSingle().Which.Should().BeEquivalentTo(
            new ParsedLink { Url = "/about", LinkText = "About us", Rel = "nofollow", Title = "About us", IsNoFollow = true });
        page.Images.Should().ContainSingle().Which.Should().BeEquivalentTo(
            new ParsedImage { Src = "/logo.png", Alt = "Logo", Width = "10", Height = "20" });
    }

    [Fact]
    public void Parse_ExtractsRobotsAndStructuredData()
    {
        var page = Parse(Page);

        page.RobotsDirectives.NoIndex.Should().BeTrue();
        page.RobotsDirectives.NoFollow.Should().BeTrue();
        page.RobotsDirectives.ForbidsFollow.Should().BeTrue();
        page.RobotsDirectives.NoArchive.Should().BeFalse();
        page.OpenGraphData.Should().Contain("og:title", "OG title");
        page.TwitterCardData.Should().Contain("twitter:card", "summary");
        page.JsonLdData.Should().Equal("""{"@type":"Thing"}""");
    }

    [Fact]
    public void Parse_OfAnEmptyDocument_ReturnsEmptyValues()
    {
        var page = Parse(string.Empty);

        page.Title.Should().BeEmpty();
        page.Links.Should().BeEmpty();
        page.BodyText.Should().BeEmpty();
        page.RobotsDirectives.ForbidsFollow.Should().BeFalse();
    }
}
