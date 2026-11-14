namespace SamoBot.Adapters.Postgres;

/// <summary>Builds a LIKE pattern from user text. The text matches literally; only the outer wildcards remain.</summary>
internal static class LikePattern
{
    public static string? Contains(string? text)
    {
        var trimmed = text?.Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            return null;
        }

        var escaped = trimmed.Replace("\\", "\\\\").Replace("%", "\\%").Replace("_", "\\_");
        return $"%{escaped}%";
    }
}
