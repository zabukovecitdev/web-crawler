using SamoBot.Core.Domain;

namespace SamoBot.Core.Parsing;

/// <summary>Applies the limits of a crawl job to the links of a page.</summary>
public static class JobScope
{
    /// <summary>Links of the page, one level deeper than the page.</summary>
    public static IReadOnlyList<NewUrl> Filter(FetchToParse parent, IReadOnlyList<string> urls) =>
        FilterAtDepth(parent, urls, parent.Depth + 1);

    /// <summary>URLs at the given depth, such as redirect targets that stay at the depth of the page.</summary>
    public static IReadOnlyList<NewUrl> FilterAtDepth(FetchToParse parent, IReadOnlyList<string> urls, int depth)
    {
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
