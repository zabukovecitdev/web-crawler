namespace SamoBot.Core.Domain;

/// <summary>The one place that says which media types the crawler stores: HTML. Other types are fetched and skipped.</summary>
public static class StoredMediaTypes
{
    /// <summary>True for a media type (no parameters) of text/html or application/xhtml+xml.</summary>
    public static bool IsStored(string? mediaType) =>
        mediaType is not null
        && (mediaType.Equals("text/html", StringComparison.OrdinalIgnoreCase)
            || mediaType.Equals("application/xhtml+xml", StringComparison.OrdinalIgnoreCase));
}
