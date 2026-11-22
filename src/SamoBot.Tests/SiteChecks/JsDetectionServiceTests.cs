using FluentAssertions;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.SiteChecks;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.SiteChecks;

public class JsDetectionServiceTests
{
    private static readonly PageShell Shell = new(84, 1, 4, 0, true, false);
    private static readonly PageShell Full = new(5902, 131, 4, 0, false, false);
    private static readonly BrowserStatus Up = new(true, null, TestData.Now);
    private static readonly BrowserStatus Down = new(false, "down", TestData.Now);

    private sealed class ShellParser(params PageShell[] shells) : SamoBot.Core.Ports.IPageParser
    {
        private int _next;

        public ParsedPage Parse(Stream content, string? contentType) => new() { Shell = shells[Math.Min(_next++, shells.Length - 1)] };
    }

    private static StartPage Page(bool reachable = true) =>
        new(new Uri("https://a.example/"), false, reachable ? 200 : 500, null, false, [1, 2], "text/html");

    private static JsDetectionService Service(ScriptedFetcher browser, params PageShell[] shells) =>
        new(new ShellParser(shells), new FetcherResolver([browser]));

    private static ScriptedFetcher Browser() => new(true, _ => ScriptedFetcher.Html());

    [Fact]
    public async Task APageWithItsContentIsNotRenderedInTheBrowser()
    {
        var browser = Browser();

        var check = await Service(browser, Full).Detect(Page(), Up, default);

        check.Verdict.Should().Be(JsVerdict.NotNeeded);
        browser.Requested.Should().BeEmpty();
    }

    [Fact]
    public async Task ASuspectPageThatIsRenderedWithMuchMoreContentNeedsJavaScript()
    {
        var browser = Browser();

        var check = await Service(browser, Shell, Full).Detect(Page(), Up, default);

        check.Verdict.Should().Be(JsVerdict.Needed);
        check.Rendered.Should().Be(Full);
        browser.Requested.Should().Equal("https://a.example/");
    }

    [Fact]
    public async Task ASuspectPageThatLooksTheSameWhenRenderedDoesNotNeedJavaScript()
    {
        var check = await Service(Browser(), Shell, Shell).Detect(Page(), Up, default);

        check.Verdict.Should().Be(JsVerdict.NotNeeded);
    }

    [Fact]
    public async Task ASuspectPageWithoutABrowserIsProbablyNeededAndNotConfirmed()
    {
        var browser = Browser();

        var check = await Service(browser, Shell).Detect(Page(), Down, default);

        check.Should().Be(new JavaScriptCheck(JsVerdict.ProbablyNeeded, Shell, null));
        browser.Requested.Should().BeEmpty();
    }

    [Fact]
    public async Task ARenderThatFailsLeavesTheVerdictAtProbablyNeeded()
    {
        var browser = new ScriptedFetcher(true, _ => new FetchResponse { StatusCode = 0, Error = "timeout" });

        var check = await Service(browser, Shell).Detect(Page(), Up, default);

        check.Verdict.Should().Be(JsVerdict.ProbablyNeeded);
    }

    [Fact]
    public async Task AStartPageThatCannotBeReadIsNotJudged()
    {
        var check = await Service(Browser(), Shell).Detect(Page(reachable: false), Up, default);

        check.Verdict.Should().Be(JsVerdict.NotNeeded);
    }
}
