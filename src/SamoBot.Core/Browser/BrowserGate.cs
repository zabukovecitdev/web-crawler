using FluentResults;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Browser;

public sealed class BrowserGate(BrowserAvailability browser)
{
    public async Task<Result> Ensure(CrawlJob job, CancellationToken ct)
    {
        if (!job.NeedsBrowser)
        {
            return Result.Ok();
        }

        var status = await browser.Refresh(ct);
        return status.Available ? Result.Ok() : Result.Fail(new BrowserUnavailableError(Explain(job)));
    }

    public static string Explain(CrawlJob job) => job.RenderMode == RenderMode.Always
        ? "The crawl renders every page in a browser, but the browser service is not available."
        : "This site needs JavaScript, but the browser service is not available.";
}
