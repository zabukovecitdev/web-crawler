using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;


public sealed record PagesResponse(int Total, IReadOnlyList<JobPage> Items);

public sealed record RobotsResponse(string Host, string Content, int? CrawlDelayMs, DateTimeOffset FetchedAt, DateTimeOffset ExpiresAt)
{
    public static RobotsResponse From(RobotsRules rules) => new(
        rules.Host,
        rules.Content,
        rules.CrawlDelay is { } delay ? (int)delay.TotalMilliseconds : null,
        rules.FetchedAt,
        rules.ExpiresAt);
}

public sealed record ExplainResponse(
    string Url,
    string Host,
    string UserAgent,
    bool AllowedByRobots,
    int? CrawlDelayMs,
    RobotsResponse RobotsTxt)
{
    public string? MatchedRule { get; init; }
}
