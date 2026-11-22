using SamoBot.Core.Domain;

namespace SamoBot.Core.SiteChecks;

public sealed record SiteCheckResult(
    string RequestedUrl,
    StartPage StartPage,
    RobotsCheck Robots,
    SitemapPlan Sitemap,
    BrowserStatus Browser,
    JavaScriptCheck JavaScript,
    DateTimeOffset CheckedAt)
{
    public string StartUrl => StartPage.Url.AbsoluteUri;

    public string Host => StartPage.Url.Authority;
}
