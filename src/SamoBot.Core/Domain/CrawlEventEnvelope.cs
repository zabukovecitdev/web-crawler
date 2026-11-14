using System.Text.Json;
using System.Text.Json.Nodes;

namespace SamoBot.Core.Domain;

/// <summary>
/// Builds the flat JSON of a crawl event: <c>{ crawlJobId, eventType, at, ...fields }</c>, plus <c>id</c> when the event is stored.
/// The SignalR push and the events endpoint use this one shape. The reserved names win over a payload field of the same name.
/// </summary>
public static class CrawlEventEnvelope
{
    private static readonly string[] Reserved = ["id", "crawlJobId", "eventType", "at"];

    public static JsonObject Build(int crawlJobId, string eventType, DateTimeOffset at, string? payloadJson, long? id = null)
    {
        var envelope = new JsonObject();
        if (id is { } eventId)
        {
            envelope["id"] = eventId;
        }

        envelope["crawlJobId"] = crawlJobId;
        envelope["eventType"] = eventType;
        envelope["at"] = at.UtcDateTime.ToString("O");
        foreach (var (name, value) in ParsePayload(payloadJson))
        {
            if (!Reserved.Contains(name))
            {
                envelope[name] = value?.DeepClone();
            }
        }

        return envelope;
    }

    private static JsonObject ParsePayload(string? payloadJson)
    {
        if (string.IsNullOrWhiteSpace(payloadJson))
        {
            return [];
        }

        try
        {
            return JsonNode.Parse(payloadJson) as JsonObject ?? [];
        }
        catch (JsonException)
        {
            return [];
        }
    }
}
