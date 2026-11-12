namespace SamoBot.Core.Domain;

public sealed record JobEvent(long Id, int CrawlJobId, string EventType, string Payload, DateTimeOffset CreatedAt);
