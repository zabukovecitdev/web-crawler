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
          <meta NAME="SamoBot" content="noarchive">
          <meta http-equiv="refresh" content="5; url=/next">
          <base href="/docs/">
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

    private static ParsedPage Parse(string body, string? contentType = "text/html") =>
        new HtmlAgilityPackParser().Parse(new MemoryStream(Encoding.UTF8.GetBytes(body)), contentType);

    private const string MarkdownPage = """
        ---
        title: "Docs & guides"
        summary: Find guides.
        related:
          - /docs/other
        ---

        # Welcome

        Some **bold** text and a [link](/docs/next "Next page").

        ## Install

        ```bash
        npm i
        ```
        """;

    [Fact]
    public void ParseOfMarkdownTakesTitleAndDescriptionFromTheFrontMatter()
    {
        var page = Parse(MarkdownPage, "text/markdown; charset=utf-8");

        page.Title.Should().Be("Docs & guides");
        page.Description.Should().Be("Find guides.");
    }

    [Fact]
    public void ParseOfMarkdownExtractsHeadingsTextAndLinks()
    {
        var page = Parse(MarkdownPage, "text/markdown");

        page.Headings.Should().Equal(new ParsedHeading(1, "Welcome"), new ParsedHeading(2, "Install"));
        page.BodyText.Should().Contain("Some bold text and a link").And.NotContain("title:").And.NotContain("/docs/other");
        page.Links.Should().ContainSingle(l => l.Url == "/docs/next" && l.LinkText == "link");
    }

    [Fact]
    public void ParseOfMarkdownWithoutFrontMatterTakesTheTitleFromTheFirstLevelOneHeading()
    {
        Parse("Intro\n\n# The *real* title\n\n# Second", "text/x-markdown").Title.Should().Be("The real title");
    }

    [Fact]
    public void ParseExtractsMetadata()
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
    public void ParseExtractsTextWithoutScriptsAndStyles()
    {
        var page = Parse(Page);

        page.Headings.Should().Equal(new ParsedHeading(1, "Main heading"), new ParsedHeading(2, "Sub"));
        page.BodyText.Should().Contain("Some text here.").And.NotContain("hidden").And.NotContain("color: red");
    }

    [Fact]
    public void ParseExtractsLinksAndImagesAndSkipsEntriesWithoutTarget()
    {
        var page = Parse(Page);

        page.Links.Should().ContainSingle().Which.Should().BeEquivalentTo(
            new ParsedLink { Url = "/about", LinkText = "About us", Rel = "nofollow", Title = "About us" });
        page.Images.Should().ContainSingle().Which.Should().BeEquivalentTo(
            new ParsedImage { Src = "/logo.png", Alt = "Logo", Width = "10", Height = "20" });
    }

    [Fact]
    public void ParseExtractsRobotsAndStructuredData()
    {
        var page = Parse(Page);

        page.MetaTags.Should().Contain(new MetaTag("robots", "noindex, nofollow"))
            .And.Contain(new MetaTag("SamoBot", "noarchive"))
            .And.Contain(new MetaTag("refresh", "5; url=/next", HttpEquiv: true))
            .And.Contain(new MetaTag("description", "A page"));
        page.BaseHref.Should().Be("/docs/");
        page.OpenGraphData.Should().Contain("og:title", "OG title");
        page.TwitterCardData.Should().Contain("twitter:card", "summary");
        page.JsonLdData.Should().Equal("""{"@type":"Thing"}""");
    }

    [Theory]
    [InlineData("""<link rel="Canonical" href="/a">""")]
    [InlineData("""<link rel="alternate canonical" href="/a">""")]
    [InlineData("""<link href="/a" rel="canonical"><link rel="canonical" href="/b">""")]
    public void ParseFindsTheCanonicalLinkWhateverTheCaseOrOtherRelValues(string head) =>
        Parse($"<html><head>{head}</head></html>").Canonical.Should().Be("/a");

    [Fact]
    public void ParseOfAnEmptyDocumentReturnsEmptyValues()
    {
        var page = Parse(string.Empty);

        page.Title.Should().BeEmpty();
        page.Links.Should().BeEmpty();
        page.BodyText.Should().BeEmpty();
        page.MetaTags.Should().BeEmpty();
        page.BaseHref.Should().BeEmpty();
    }

    [Fact]
    public void ParseOfPlainTextKeepsTheTextAsBodyText()
    {
        var page = Parse("User-agent notes\n<not a tag> & more", "text/plain; charset=utf-8");

        page.BodyText.Should().Be("User-agent notes <not a tag> & more");
        page.Title.Should().BeEmpty();
        page.Links.Should().BeEmpty();
    }

    [Fact]
    public void ParseOfPlainTextThatStartsLikeHtmlParsesItAsHtml()
    {
        var page = Parse("<!doctype html><html><head><title>Wrong type</title></head><body><a href=\"/x\">x</a></body></html>", "text/plain");

        page.Title.Should().Be("Wrong type");
        page.Links.Should().ContainSingle(l => l.Url == "/x");
    }

    [Fact]
    public void TheShellCountsTextLinksAndScripts()
    {
        var shell = Parse(Page).Shell;

        (shell.TextLength > 0, shell.LinkCount, shell.ScriptCount, shell.InlineScriptBytes).Should().Be((true, 1, 1, "var hidden = 1;".Length));
        (shell.EmptyAppContainer, shell.NoscriptNotice).Should().Be((false, false));
    }

    [Theory]
    [InlineData("<div id=\"root\"></div>", true)]
    [InlineData("<div id=\"app\">   </div>", true)]
    [InlineData("<div id=\"__next\"><script>x</script></div>", true)]
    [InlineData("<app-root></app-root>", true)]
    [InlineData("<div id=\"root\"><main><p>Hello</p></main></div>", false)]
    [InlineData("<div id=\"other\"></div>", false)]
    public void AnEmptyAppContainerIsRecognised(string body, bool expected) =>
        Parse($"<html><body>{body}<script src=\"/a.js\"></script></body></html>").Shell.EmptyAppContainer.Should().Be(expected);

    [Theory]
    [InlineData("You need to enable JavaScript to run this app.", true)]
    [InlineData("JavaScript is required for this site", true)]
    [InlineData("<img src=\"https://t.example/pixel.gif\">", false)]
    public void ANoscriptNoticeAboutJavaScriptIsRecognised(string text, bool expected) =>
        Parse($"<html><body><noscript>{text}</noscript><p>x</p></body></html>").Shell.NoscriptNotice.Should().Be(expected);

    [Fact]
    public void ExternalScriptsCountButAddNoInlineBytes()
    {
        var shell = Parse("<html><body><script src=\"/a.js\"></script><script src=\"/b.js\"></script></body></html>").Shell;

        (shell.ScriptCount, shell.InlineScriptBytes).Should().Be((2, 0));
    }
}
