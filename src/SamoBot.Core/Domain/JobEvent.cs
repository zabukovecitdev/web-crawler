namespace SamoBot.Core.Domain;

public sealed record JobEvent
{
    public long Id { get; init; }
    public int CrawlJobId { get; init; }
    public string EventType { get; init; } = string.Empty;
    public string Payload { get; init; } = "{}";
    public DateTimeOffset CreatedAt { get; init; }
}
