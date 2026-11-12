namespace SamoBot.Core.Ports;

public interface ICrawlEventPublisher
{
    Task Publish(int crawlJobId, string eventType, object payload, CancellationToken ct);
}
