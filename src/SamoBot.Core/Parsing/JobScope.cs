using SamoBot.Core.Domain;

namespace SamoBot.Core.Parsing;

/// <summary>Applies the limits of a crawl job to the links of a page.</summary>
public static class JobScope
{
    public static IReadOnlyList<NewUrl> Filter(FetchToParse parent, IReadOnlyList<string> urls)
    {
        var depth = parent.Depth + 1;
        if (parent.MaxDepth is { } maxDepth && depth > maxDepth)
        {
            return [];
        }

        return urls
            .Select(url => new Uri(url))
            .Where(uri => IsAllowedHost(parent, uri.Authority))
            .Select(uri => new NewUrl(parent.CrawlJobId, uri.AbsoluteUri, uri.AbsoluteUri, uri.Authority, depth))
            .ToList();
    }

    private static bool IsAllowedHost(FetchToParse parent, string host) =>
        parent.AllowedHosts.Contains(host, StringComparer.OrdinalIgnoreCase);
}
