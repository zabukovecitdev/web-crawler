using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace SamoBot.Adapters.Storage;

/// <summary>Builds the object name of a stored page: domain, path and a hash of the URL.</summary>
internal static partial class ObjectNameGenerator
{
    private static readonly Dictionary<string, string> ExtensionsByMediaType = new(StringComparer.OrdinalIgnoreCase)
    {
        ["text/html"] = "html",
        ["application/xhtml+xml"] = "html",
        ["text/plain"] = "txt",
        ["application/json"] = "json",
        ["application/xml"] = "xml",
        ["text/xml"] = "xml",
        ["text/css"] = "css",
        ["text/javascript"] = "js",
        ["application/javascript"] = "js",
        ["application/pdf"] = "pdf",
        ["image/jpeg"] = "jpg",
        ["image/png"] = "png",
        ["image/gif"] = "gif",
        ["image/webp"] = "webp",
        ["image/svg+xml"] = "svg",
        ["application/rss+xml"] = "rss",
        ["application/atom+xml"] = "atom"
    };

    private static readonly HashSet<string> PathExtensions = new(StringComparer.OrdinalIgnoreCase)
    {
        "html", "htm", "xml", "json", "txt", "css", "js", "pdf", "jpg", "jpeg", "png", "gif", "webp", "svg"
    };

    public static string GenerateHierarchical(string url, string? contentType = null)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            return $"{UrlHash(url)}.{ExtensionFromContentType(contentType) ?? "html"}";
        }

        var extension = ExtensionFromContentType(contentType) ?? ExtensionFromPath(uri) ?? "html";
        var objectName = $"{Sanitize(uri.Host)}/{SanitizePath(uri.AbsolutePath)}/{UrlHash(url)}.{extension}";
        return MultipleSlashes().Replace(objectName, "/");
    }

    private static string SanitizePath(string absolutePath)
    {
        var path = Sanitize(absolutePath.Trim('/'));
        return path.Length == 0 ? "index" : path;
    }

    private static string Sanitize(string value) => InvalidChars().Replace(value, "_");

    private static string UrlHash(string url) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(url))).ToLowerInvariant();

    private static string? ExtensionFromContentType(string? contentType)
    {
        if (string.IsNullOrWhiteSpace(contentType))
        {
            return null;
        }

        var mediaType = contentType.Split(';')[0].Trim();
        return ExtensionsByMediaType.GetValueOrDefault(mediaType);
    }

    private static string? ExtensionFromPath(Uri uri)
    {
        var extension = Path.GetExtension(uri.AbsolutePath).TrimStart('.');
        return PathExtensions.Contains(extension) ? extension.ToLowerInvariant() : null;
    }

    [GeneratedRegex(@"[^a-zA-Z0-9\-_./]")]
    private static partial Regex InvalidChars();

    [GeneratedRegex(@"/{2,}")]
    private static partial Regex MultipleSlashes();
}
