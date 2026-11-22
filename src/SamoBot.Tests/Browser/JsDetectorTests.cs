using FluentAssertions;
using SamoBot.Core.Browser;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Browser;

public class JsDetectorTests
{
    private static PageShell Page(int text = 5000, int links = 120, int scripts = 2, int inline = 0, bool empty = false, bool notice = false) =>
        new(text, links, scripts, inline, empty, notice);

    [Fact]
    public void AFullPageWithPlentyOfTextAndLinksIsNotSuspect() => JsDetector.IsSuspect(Page()).Should().BeFalse();

    [Fact]
    public void AnEmptyAppContainerIsSuspect() => JsDetector.IsSuspect(Page(text: 4000, empty: true)).Should().BeTrue();

    [Fact]
    public void ANoscriptNoticeOnAThinPageIsSuspect() => JsDetector.IsSuspect(Page(text: 80, links: 30, notice: true)).Should().BeTrue();

    [Fact]
    public void ANoscriptNoticeOnAPageFullOfTextIsNot() => JsDetector.IsSuspect(Page(text: 8000, notice: true)).Should().BeFalse();

    [Theory]
    [InlineData(120, 5, 0, true)]
    [InlineData(120, 2, 12_000, true)]
    [InlineData(120, 2, 500, false)]
    [InlineData(2000, 9, 90_000, false)]
    public void ThinTextWithHeavyScriptsIsSuspect(int text, int scripts, int inline, bool expected) =>
        JsDetector.IsSuspect(Page(text: text, links: 40, scripts: scripts, inline: inline)).Should().Be(expected);

    [Theory]
    [InlineData(1, 120, 1, true)]
    [InlineData(2, 400, 1, true)]
    [InlineData(3, 400, 1, false)]
    [InlineData(1, 120, 0, false)]
    [InlineData(1, 900, 1, false)]
    public void AlmostNoLinksOnAThinPageWithAScriptIsSuspect(int links, int text, int scripts, bool expected) =>
        JsDetector.IsSuspect(Page(text: text, links: links, scripts: scripts)).Should().Be(expected);

    [Fact]
    public void ATinyStaticPageWithoutScriptsIsNotSuspect() => JsDetector.IsSuspect(Page(text: 20, links: 0, scripts: 0)).Should().BeFalse();

    [Theory]
    [InlineData(84, 1, 5902, 131, true)]
    [InlineData(84, 1, 300, 1, false)]
    [InlineData(1000, 40, 1400, 45, false)]
    [InlineData(1000, 40, 2600, 40, true)]
    [InlineData(1000, 40, 1100, 90, true)]
    [InlineData(1000, 40, 1000, 40, false)]
    [InlineData(100, 3, 700, 5, true)]
    [InlineData(100, 3, 400, 5, false)]
    public void TheRenderedPageNeedsClearlyMoreTextOrLinksToProveJavaScript(int plainText, int plainLinks, int renderedText, int renderedLinks, bool expected) =>
        JsDetector.NeedsJs(Page(text: plainText, links: plainLinks), Page(text: renderedText, links: renderedLinks)).Should().Be(expected);
}
