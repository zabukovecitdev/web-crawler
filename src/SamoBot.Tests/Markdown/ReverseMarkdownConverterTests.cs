using FluentAssertions;
using SamoBot.Adapters.Markdown;

namespace SamoBot.Tests.Markdown;

public class ReverseMarkdownConverterTests
{
    private static readonly Uri PageUrl = new("https://ex.com/x/");

    private static string Convert(string html, Uri? url = null) => new ReverseMarkdownConverter().Convert(html, url ?? PageUrl);

    private static string Body(string inner) => $"<html><head><title>T</title></head><body>{inner}</body></html>";

    [Fact]
    public void HeadingsBecomeAtxHeadings()
    {
        var markdown = Convert(Body("<h1>One</h1><h2>Two</h2><h3>Three</h3><h4>Four</h4><h5>Five</h5><h6>Six</h6>"));

        markdown.Should().Be("# One\n\n## Two\n\n### Three\n\n#### Four\n\n##### Five\n\n###### Six\n");
    }

    [Fact]
    public void ScriptsStylesNoscriptSvgIframesFormsAndCommentsAreRemoved()
    {
        var markdown = Convert(Body(
            "<p>Kept</p><script>var scriptText=1;</script><style>.styleText{}</style><noscript>noscriptText</noscript>"
            + "<svg><text>svgText</text></svg><iframe src=\"/f\">iframeText</iframe><form><label>formText</label><input value=\"inputText\"></form>"
            + "<!-- commentText -->"));

        markdown.Should().Be("Kept\n");
    }

    [Fact]
    public void NavigationAsideAndFooterAreRemovedButTheHeaderHeadingStays()
    {
        var markdown = Convert(Body(
            "<header><nav><a href=\"/home\">navText</a></nav><h1>Title</h1></header>"
            + "<div role=\"navigation\">roleNavText</div><aside>asideText</aside><p>Body</p><footer>footerText</footer>"));

        markdown.Should().Contain("# Title").And.Contain("Body")
            .And.NotContain("navText").And.NotContain("roleNavText").And.NotContain("asideText").And.NotContain("footerText");
    }

    [Fact]
    public void MainIsTheRootWhenThePageHasOne() =>
        Convert(Body("<p>outside</p><main><p>inside</p></main><p>after</p>")).Should().Be("inside\n");

    [Fact]
    public void ASingleArticleIsTheRoot() =>
        Convert(Body("<p>outside</p><article><p>inside</p></article>")).Should().Be("inside\n");

    [Fact]
    public void TheWholeBodyIsKeptWhenThereAreTwoArticles() =>
        Convert(Body("<p>outside</p><article><p>one</p></article><article><p>two</p></article>"))
            .Should().Contain("outside").And.Contain("one").And.Contain("two");

    [Fact]
    public void RelativeImageAndLinkReferencesBecomeAbsolute()
    {
        var markdown = Convert(Body("<img src=\"/a.png\" alt=\"A\"><a href=\"rel/page\">Go</a>"));

        markdown.Should().Contain("![A](https://ex.com/a.png)").And.Contain("[Go](https://ex.com/x/rel/page)");
    }

    [Fact]
    public void ABaseHrefIsTheBaseOfRelativeReferences()
    {
        var markdown = Convert("<html><head><base href=\"https://cdn.example/root/\"></head><body><a href=\"p\">Go</a><img src=\"i.png\" alt=\"I\"></body></html>");

        markdown.Should().Contain("[Go](https://cdn.example/root/p)").And.Contain("![I](https://cdn.example/root/i.png)");
    }

    [Fact]
    public void ALazyImageUsesItsDataSource() =>
        Convert(Body("<img src=\"data:image/gif;base64,R0lG\" data-src=\"/lazy.png\" alt=\"L\">")).Should().Contain("![L](https://ex.com/lazy.png)");

    [Theory]
    [InlineData("<img src=\"data:image/png;base64,AAAA\" alt=\"inline\">")]
    [InlineData("<img alt=\"nothing\">")]
    public void ImagesWithoutAWebUrlAreRemoved(string image) =>
        Convert(Body("<p>Text</p>" + image)).Should().Be("Text\n");

    [Fact]
    public void LinksWithOtherSchemesOrAnchorsBecomePlainText()
    {
        var markdown = Convert(Body("<p><a href=\"javascript:alert(1)\">script</a> <a href=\"#top\">top</a> <a href=\"mailto:a@ex.com\">mail</a></p>"));

        markdown.Should().Contain("script top").And.NotContain("javascript").And.NotContain("#top").And.Contain("[mail](mailto:a@ex.com)");
    }

    [Fact]
    public void AnchorsWithoutTextAreRemoved() =>
        Convert(Body("<p>Text<a href=\"/x\"></a></p>")).Should().Be("Text\n");

    [Fact]
    public void ListsConvertWithDashBullets()
    {
        var markdown = Convert(Body("<ul><li>a<ul><li>b</li></ul></li><li>c</li></ul><ol><li>one</li><li>two</li></ol>"));

        markdown.Should().Contain("- a").And.Contain("  - b").And.Contain("- c").And.Contain("1. one").And.Contain("2. two");
    }

    [Fact]
    public void TablesBecomePipeTables()
    {
        var markdown = Convert(Body("<table><thead><tr><th>A</th><th>B</th></tr></thead><tbody><tr><td>1</td><td>2</td></tr></tbody></table>"));

        markdown.Should().Contain("| A | B |").And.Contain("| --- | --- |").And.Contain("| 1 | 2 |");
    }

    [Fact]
    public void CodeBlocksAreFencedAndBlockQuotesArePrefixed()
    {
        var markdown = Convert(Body("<pre><code class=\"language-js\">let a = 1;</code></pre><blockquote><p>quoted</p></blockquote>"));

        markdown.Should().Contain("```js").And.Contain("let a = 1;").And.Contain("> quoted");
    }

    [Fact]
    public void EntitiesAreDecodedAndMarkdownCharactersAreEscaped()
    {
        var markdown = Convert(Body("<p>Tom &amp; Jerry</p><p>1. not a list</p>"));

        markdown.Should().Contain("Tom & Jerry").And.NotContain("&amp;").And.Contain("1\\. not a list");
    }

    [Fact]
    public void OutputIsTidy()
    {
        var markdown = Convert(Body("<p>a</p><br><br><br><br><p>b   </p><div> </div><p>c</p>"));

        markdown.Should().NotContain("\n\n\n").And.NotContain(" \n").And.EndWith("c\n").And.NotEndWith("\n\n");
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("<html><body><script>x()</script><style>p{}</style></body></html>")]
    public void NothingReadableGivesAnEmptyString(string html) =>
        Convert(html).Should().BeEmpty();

    [Fact]
    public void HiddenElementsAreRemoved()
    {
        var markdown = Convert(Body(
            "<p>Shown</p><p hidden>hiddenAttr</p><div aria-hidden=\"true\">ariaHidden</div><p style=\"color:red; display: none\">styleHidden</p>"));

        markdown.Should().Be("Shown\n");
    }
}
