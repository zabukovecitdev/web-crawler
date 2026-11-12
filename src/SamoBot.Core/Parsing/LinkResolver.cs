using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Parsing;

/// <summary>Turns the links of a page into absolute, normalized, unique http and https URLs.</summary>
public static class LinkResolver
{
    public static IReadOnlyList<string> Resolve(Uri pageUrl, IEnumerable<ParsedLink> links) =>
        links
            .Select(link => ResolveOne(pageUrl, link.Url))
            .OfType<string>()
            .Distinct()
            .ToList();

    private static string? ResolveOne(Uri pageUrl, string href)
    {
        // The base-URI overload is the safe form of new Uri(pageUrl, href).
        // Uri.TryCreate(href, UriKind.Absolute) would turn "/path" into a file:// URI on Linux.
        if (string.IsNullOrWhiteSpace(href) || !Uri.TryCreate(pageUrl, href, out var absolute))
        {
            return null;
        }

        if (absolute.Scheme is not ("http" or "https") || !UrlNormalizer.TryClean(absolute.AbsoluteUri, out var clean))
        {
            return null;
        }

        return clean!.AbsoluteUri;
    }
}
