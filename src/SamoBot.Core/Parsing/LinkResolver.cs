using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Parsing;

public static class LinkResolver
{
    public static IReadOnlyList<string> Resolve(Uri pageUrl, IEnumerable<ParsedLink> links) =>
        links
            .Select(link => ResolveOne(pageUrl, link.Url))
            .OfType<string>()
            .Distinct()
            .ToList();

    public static string? Normalize(Uri url) => ResolveOne(url, url.AbsoluteUri);

    public static string? ResolveOne(Uri pageUrl, string href)
    {
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
