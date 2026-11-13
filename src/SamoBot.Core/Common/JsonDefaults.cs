using System.Text.Json;

namespace SamoBot.Core.Common;

public static class JsonDefaults
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web);
}
