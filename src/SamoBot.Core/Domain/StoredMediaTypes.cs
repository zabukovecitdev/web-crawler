namespace SamoBot.Core.Domain;

public static class StoredMediaTypes
{
    private const int SniffLength = 4096;

    public static readonly IReadOnlyList<string> All =
        ["text/html", "application/xhtml+xml", "text/markdown", "text/x-markdown", "text/plain"];

    public static bool IsStored(string? mediaType) => mediaType is not null && All.Contains(mediaType, StringComparer.OrdinalIgnoreCase);

    public static bool IsHtml(string? mediaType) =>
        mediaType is not null
        && (mediaType.Equals("text/html", StringComparison.OrdinalIgnoreCase)
            || mediaType.Equals("application/xhtml+xml", StringComparison.OrdinalIgnoreCase));

    public static bool IsMarkdown(string? mediaType) =>
        mediaType is not null
        && (mediaType.Equals("text/markdown", StringComparison.OrdinalIgnoreCase)
            || mediaType.Equals("text/x-markdown", StringComparison.OrdinalIgnoreCase));

    public static bool IsPlainText(string? mediaType) =>
        mediaType is not null && mediaType.Equals("text/plain", StringComparison.OrdinalIgnoreCase);

    public static string FileExtension(string? contentType)
    {
        var mediaType = MediaType(contentType);
        if (IsMarkdown(mediaType))
        {
            return "md";
        }

        return IsPlainText(mediaType) ? "txt" : "html";
    }

    public static string? MediaType(string? contentType)
    {
        var mediaType = contentType?.Split(';')[0].Trim();
        return string.IsNullOrEmpty(mediaType) ? null : mediaType;
    }

    public static bool LooksLikeHtml(string text)
    {
        var snippet = text.Length > SniffLength ? text[..SniffLength] : text;
        return snippet.Contains("<!doctype html", StringComparison.OrdinalIgnoreCase)
               || snippet.Contains("<html", StringComparison.OrdinalIgnoreCase);
    }
}
