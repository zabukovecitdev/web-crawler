using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum UrlStatus
{
    Idle,
    InFlight,
    Disabled
}
