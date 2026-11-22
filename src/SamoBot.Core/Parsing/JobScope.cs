using SamoBot.Core.Domain;
using SamoBot.Core.Scope;

namespace SamoBot.Core.Parsing;

public static class JobScope
{
    public static IReadOnlyList<NewUrl> Filter(FetchToParse parent, IReadOnlyList<string> urls) =>
        parent.FollowLinkedPages ? FilterAtDepth(parent, urls, parent.Depth + 1) : [];

    public static IReadOnlyList<NewUrl> FilterAtDepth(FetchToParse parent, IReadOnlyList<string> urls, int depth)
    {
        if (parent.MaxDepth is { } maxDepth && depth > maxDepth)
        {
            return [];
        }

        var paths = new PathRules(parent.PathRules);
        return urls
            .Select(url => new Uri(url))
            .Where(uri => IsAllowedHost(parent, uri.Authority) && paths.Allows(uri))
            .Select(uri => new NewUrl(parent.CrawlJobId, uri.AbsoluteUri, uri.AbsoluteUri, uri.Authority, depth))
            .ToList();
    }

    private static bool IsAllowedHost(FetchToParse parent, string host) =>
        parent.AllowedHosts.Contains(host, StringComparer.OrdinalIgnoreCase);
}
