using RobotsTxtParser;

namespace SamoBot.Core.Domain;

/// <summary>The robots.txt rules of one host. The constructor parses the content one time.</summary>
public sealed class RobotsRules
{
    private const string AllowAllContent = "User-agent: *\nDisallow:\n";
    private const string DisallowAllContent = "User-agent: *\nDisallow: /\n";

    private readonly Robots _robots;

    public RobotsRules(string host, string content, DateTimeOffset fetchedAt, DateTimeOffset expiresAt)
    {
        Host = host;
        Content = content;
        FetchedAt = fetchedAt;
        ExpiresAt = expiresAt;
        _robots = Robots.Load(content);
        CrawlDelay = ReadCrawlDelay(_robots);
    }

    public string Host { get; }
    public string Content { get; }
    public DateTimeOffset FetchedAt { get; }
    public DateTimeOffset ExpiresAt { get; }
    public TimeSpan? CrawlDelay { get; }

    public bool IsAllowed(string pathAndQuery, string userAgent) => _robots.IsPathAllowed(userAgent, pathAndQuery);

    public static RobotsRules AllowAll(string host, DateTimeOffset fetchedAt, TimeSpan ttl) =>
        new(host, AllowAllContent, fetchedAt, fetchedAt + ttl);

    public static RobotsRules DisallowAll(string host, DateTimeOffset fetchedAt, TimeSpan ttl) =>
        new(host, DisallowAllContent, fetchedAt, fetchedAt + ttl);

    private static TimeSpan? ReadCrawlDelay(Robots robots)
    {
        var delay = robots.CrawlDelay("*");
        return delay > TimeSpan.Zero ? delay : null;
    }
}
