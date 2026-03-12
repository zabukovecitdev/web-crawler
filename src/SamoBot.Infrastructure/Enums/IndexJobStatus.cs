using System.Text.Json.Serialization;

namespace Samobot.Infrastructure.Enums;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum IndexJobStatus
{
    Pending = 0,
    InProgress = 1,
    Completed = 2,
    Failed = 3
}
