using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;

namespace SamoBot.Core.SiteChecks;

public sealed record RobotsCheck(bool StartAllowed, double? CrawlDelaySeconds, IReadOnlyList<string> Disallowed)
{
    public const int MaxListed = 25;

    public static RobotsCheck From(RobotsRules rules, Uri start) => new(
        rules.IsAllowed(start.PathAndQuery, RobotsGate.UserAgent),
        rules.CrawlDelay?.TotalSeconds,
        RobotsFacts.Disallowed(rules.Content).Take(MaxListed).ToList());
}
