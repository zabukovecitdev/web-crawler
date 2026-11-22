using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum RenderMode
{
    Auto,
    Always,
    Never
}

public static class RenderModes
{
    public static RenderMode FromRequest(string? mode, bool? legacyUseJsRendering)
    {
        if (Enum.TryParse<RenderMode>(mode, ignoreCase: true, out var parsed) && Enum.IsDefined(parsed))
        {
            return parsed;
        }

        return legacyUseJsRendering switch
        {
            true => RenderMode.Always,
            false => RenderMode.Never,
            null => RenderMode.Auto
        };
    }

    public static bool NeedsBrowser(RenderMode mode, bool jsNeeded) =>
        mode == RenderMode.Always || (mode == RenderMode.Auto && jsNeeded);
}
