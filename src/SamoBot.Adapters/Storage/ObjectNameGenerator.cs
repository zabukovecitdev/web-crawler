using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace SamoBot.Adapters.Storage;

internal static partial class ObjectNameGenerator
{
    public static string GenerateHierarchical(string url, string extension)
    {
        if (!Uri.TryCreate(url, UriKind.Absolute, out var uri))
        {
            return $"{UrlHash(url)}.{extension}";
        }

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

    [GeneratedRegex(@"[^a-zA-Z0-9\-_./]")]
    private static partial Regex InvalidChars();

    [GeneratedRegex(@"/{2,}")]
    private static partial Regex MultipleSlashes();
}
