using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeEventPublisher : ICrawlEventPublisher
{
    public List<(int JobId, string EventType, object Payload)> Published { get; } = [];

    public Task Publish(int crawlJobId, string eventType, object payload, CancellationToken ct)
    {
        Published.Add((crawlJobId, eventType, payload));
        return Task.CompletedTask;
    }
}
