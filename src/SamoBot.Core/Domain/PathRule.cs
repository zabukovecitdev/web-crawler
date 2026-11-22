using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum PathRuleKind
{
    Include,
    Exclude
}

public sealed record PathRule(PathRuleKind Kind, string Prefix, bool Exact = false);

public sealed record PathRuleEffect(int Stopped, int Resumed);
