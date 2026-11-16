namespace SamoBot.Core.Domain;

/// <summary>
/// The one place that says which media types the crawler stores and parses: HTML, Markdown and plain text.
/// Other types are fetched and skipped.
/// </summary>
public static class StoredMediaTypes
{
    private const int SniffLength = 4096;

    /// <summary>Every stored media type, in lower case and without parameters.</summary>
    public static readonly IReadOnlyList<string> All =
        ["text/html", "application/xhtml+xml", "text/markdown", "text/x-markdown", "text/plain"];

    /// <summary>True for a media type (no parameters) that the crawler stores and parses.</summary>
    public static bool IsStored(string? mediaType) => mediaType is not null && All.Contains(mediaType, StringComparer.OrdinalIgnoreCase);

    /// <summary>True for text/html or application/xhtml+xml.</summary>
    public static bool IsHtml(string? mediaType) =>
        mediaType is not null
        && (mediaType.Equals("text/html", StringComparison.OrdinalIgnoreCase)
            || mediaType.Equals("application/xhtml+xml", StringComparison.OrdinalIgnoreCase));

    /// <summary>True for text/markdown (RFC 7763) or the older text/x-markdown.</summary>
    public static bool IsMarkdown(string? mediaType) =>
        mediaType is not null
        && (mediaType.Equals("text/markdown", StringComparison.OrdinalIgnoreCase)
            || mediaType.Equals("text/x-markdown", StringComparison.OrdinalIgnoreCase));

    /// <summary>True for text/plain.</summary>
    public static bool IsPlainText(string? mediaType) =>
        mediaType is not null && mediaType.Equals("text/plain", StringComparison.OrdinalIgnoreCase);

    /// <summary>The media type of a Content-Type value without its parameters, or null when it is empty.</summary>
    public static string? MediaType(string? contentType)
    {
        var mediaType = contentType?.Split(';')[0].Trim();
        return string.IsNullOrEmpty(mediaType) ? null : mediaType;
    }

    /// <summary>True when the start of a body is HTML, whatever its media type says.</summary>
    public static bool LooksLikeHtml(string text)
    {
        var snippet = text.Length > SniffLength ? text[..SniffLength] : text;
        return snippet.Contains("<!doctype html", StringComparison.OrdinalIgnoreCase)
               || snippet.Contains("<html", StringComparison.OrdinalIgnoreCase);
    }
}
