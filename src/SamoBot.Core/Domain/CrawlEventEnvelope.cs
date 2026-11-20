using System.Text.Json;
using System.Text.Json.Nodes;

namespace SamoBot.Core.Domain;

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
