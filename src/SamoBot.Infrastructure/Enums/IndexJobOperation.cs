using System.Text.Json.Serialization;

namespace Samobot.Infrastructure.Enums;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum IndexJobOperation
{
    Index = 0,
    Delete = 1,
    Reindex = 2
}
