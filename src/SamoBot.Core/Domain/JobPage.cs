namespace SamoBot.Core.Domain;

/// <summary>One frontier URL of a job, as the dashboard shows it. The property names match the existing API.</summary>
public sealed record JobPage
{
    public int Id { get; init; }
    public string Host { get; init; } = string.Empty;
    public string Url { get; init; } = string.Empty;
    public string? NormalizedUrl { get; init; }
    public UrlStatus Status { get; init; }
    public DateTimeOffset? LastCrawlAt { get; init; }
    public DateTimeOffset? NextCrawlAt { get; init; }
    public int FailCount { get; init; }
    public string? FailReason { get; init; }
    public DateTimeOffset DiscoveredAt { get; init; }
    public int Priority { get; init; }
    public int? LastFetchId { get; init; }
    public DateTimeOffset? StatusUpdatedAt { get; init; }
    public int? CrawlJobId { get; init; }
    public int Depth { get; init; }
    public bool UseJsRendering { get; init; }
    public bool RespectRobots { get; init; }
}
