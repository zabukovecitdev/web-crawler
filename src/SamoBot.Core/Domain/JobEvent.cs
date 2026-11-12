namespace SamoBot.Core.Domain;

/// <summary>One event of a crawl job. The property names match the existing API.</summary>
public sealed record JobEvent
{
    public long Id { get; init; }
    public int CrawlJobId { get; init; }
    public string EventType { get; init; } = string.Empty;
    public string Payload { get; init; } = "{}";
    public DateTimeOffset CreatedAt { get; init; }
}
