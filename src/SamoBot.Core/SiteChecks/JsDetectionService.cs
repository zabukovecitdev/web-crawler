using SamoBot.Core.Browser;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.SiteChecks;

public sealed class JsDetectionService(IPageParser parser, FetcherResolver fetchers)
{
    public async Task<JavaScriptCheck> Detect(StartPage page, BrowserStatus browser, CancellationToken ct)
    {
        if (!page.Reachable || page.Content is null)
        {
            return new JavaScriptCheck(JsVerdict.NotNeeded, PageShell.Empty, null);
        }

        var plain = Shell(page.Content, page.ContentType);
        if (!JsDetector.IsSuspect(plain))
        {
            return new JavaScriptCheck(JsVerdict.NotNeeded, plain, null);
        }

        return browser.Available ? await Compare(page, plain, ct) : new JavaScriptCheck(JsVerdict.ProbablyNeeded, plain, null);
    }

    private async Task<JavaScriptCheck> Compare(StartPage page, PageShell plain, CancellationToken ct)
    {
        var url = ProbeUrl.For(page.Url, true);
        var response = await fetchers.For(url).Fetch(url, ct);
        if (!response.IsSuccess || response.Content is null)
        {
            return new JavaScriptCheck(JsVerdict.ProbablyNeeded, plain, null);
        }

        var rendered = Shell(response.Content, response.ContentType);
        return new JavaScriptCheck(JsDetector.NeedsJs(plain, rendered) ? JsVerdict.Needed : JsVerdict.NotNeeded, plain, rendered);
    }

    private PageShell Shell(byte[] content, string? contentType)
    {
        using var stream = new MemoryStream(content);
        return parser.Parse(stream, contentType).Shell;
    }
}
