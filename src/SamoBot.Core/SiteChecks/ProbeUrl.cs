using SamoBot.Core.Domain;

namespace SamoBot.Core.SiteChecks;

public static class ProbeUrl
{
    public static ClaimedUrl For(Uri url, bool inBrowser) => new()
    {
        Id = 0,
        CrawlJobId = 0,
        Host = url.Authority,
        Url = url.AbsoluteUri,
        NormalizedUrl = url.AbsoluteUri,
        Depth = 0,
        FailCount = 0,
        UseJsRendering = inBrowser
    };
}
