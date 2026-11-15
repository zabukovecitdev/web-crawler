using SamoBot.Core.Crawling;

namespace SamoBot.Core.Directives;

/// <summary>The name sites use to address this crawler, in robots.txt, a robots meta tag or an X-Robots-Tag value.</summary>
public static class CrawlerIdentity
{
    public const string Name = RobotsGate.UserAgent;
}
