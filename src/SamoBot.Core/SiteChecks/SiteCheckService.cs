using FluentResults;
using SamoBot.Core.Browser;
using SamoBot.Core.Common;
using SamoBot.Core.Crawling;
using SamoBot.Core.Jobs;

namespace SamoBot.Core.SiteChecks;

public sealed class SiteCheckService(
    StartPageResolver startPages,
    RobotsGate robots,
    SitemapPlanner sitemaps,
    BrowserAvailability browser,
    JsDetectionService javaScript,
    TimeProvider timeProvider)
{
    public async Task<Result<SiteCheckResult>> Check(string url, CancellationToken ct)
    {
        var problems = SeedUrlValidator.Validate([url]);
        if (problems.Count > 0)
        {
            return Result.Fail<SiteCheckResult>(problems.Select(problem => new Error(problem)));
        }

        var page = await startPages.Resolve(UrlNormalizer.Clean(url.Trim()).Value, ct);
        var rules = await robots.RulesFor(page.Url.Authority, ct);
        var plan = await sitemaps.Ensure(page.Url.Authority, ct);
        var status = await browser.Refresh(ct);
        var script = await javaScript.Detect(page, status, ct);
        return Result.Ok(new SiteCheckResult(url.Trim(), page, RobotsCheck.From(rules, page.Url), plan, status, script, timeProvider.GetUtcNow()));
    }
}
