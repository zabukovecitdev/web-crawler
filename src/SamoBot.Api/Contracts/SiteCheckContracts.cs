using SamoBot.Core.Domain;
using SamoBot.Core.SiteChecks;

namespace SamoBot.Api.Contracts;

public sealed record StartPageDto(int StatusCode, bool Reachable, bool BlockedByRobots, string? Problem);

public sealed record RobotsCheckDto(bool StartAllowed, double? CrawlDelaySeconds, IReadOnlyList<string> Disallowed);

public sealed record SitemapPlanDto(SitemapPlanState State, int Files, int Urls, DateTimeOffset? CheckedAt)
{
    public static SitemapPlanDto From(SitemapPlan plan) => new(plan.State, plan.Files, plan.Urls, plan.CheckedAt);
}

public sealed record BrowserStatusDto(bool Available, string? Reason, DateTimeOffset CheckedAt)
{
    public static BrowserStatusDto From(BrowserStatus status) => new(status.Available, status.Reason, status.CheckedAt);
}

public sealed record PageShellDto(int TextLength, int LinkCount, bool EmptyAppContainer)
{
    public static PageShellDto From(PageShell shell) => new(shell.TextLength, shell.LinkCount, shell.EmptyAppContainer);
}

public sealed record JavaScriptCheckDto(JsVerdict Verdict, PageShellDto Plain, PageShellDto? Rendered);

public sealed record SiteCheckDto(
    string RequestedUrl,
    string StartUrl,
    string Host,
    bool Redirected,
    StartPageDto StartPage,
    RobotsCheckDto Robots,
    SitemapPlanDto Sitemap,
    BrowserStatusDto Browser,
    JavaScriptCheckDto JavaScript,
    DateTimeOffset CheckedAt)
{
    public static SiteCheckDto From(SiteCheckResult result) => new(
        result.RequestedUrl,
        result.StartUrl,
        result.Host,
        result.StartPage.Redirected,
        new StartPageDto(result.StartPage.StatusCode, result.StartPage.Reachable, result.StartPage.BlockedByRobots, result.StartPage.Problem),
        new RobotsCheckDto(result.Robots.StartAllowed, result.Robots.CrawlDelaySeconds, result.Robots.Disallowed),
        SitemapPlanDto.From(result.Sitemap),
        BrowserStatusDto.From(result.Browser),
        new JavaScriptCheckDto(
            result.JavaScript.Verdict,
            PageShellDto.From(result.JavaScript.Plain),
            result.JavaScript.Rendered is null ? null : PageShellDto.From(result.JavaScript.Rendered)),
        result.CheckedAt);
}
