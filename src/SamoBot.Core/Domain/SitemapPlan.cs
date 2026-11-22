using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SitemapPlanState
{
    Ready,
    Pending,
    None,
    Failed
}

public sealed record SitemapPlanStatus(
    int Files,
    int Urls,
    DateTimeOffset? RequestedAt,
    DateTimeOffset? CompletedAt,
    int Attempts);

public sealed record SitemapPlan(SitemapPlanState State, int Files, int Urls, DateTimeOffset? CheckedAt);
