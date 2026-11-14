using SamoBot.Core.Domain;

namespace SamoBot.Core.Sitemaps;

/// <summary>Finds the sitemap URLs of a host: the Sitemap lines of robots.txt, or /sitemap.xml when there are none.</summary>
public static class SitemapDiscovery
{
    private const string Directive = "sitemap:";

    public static IReadOnlyList<string> Seeds(RobotsRules rules, string host)
    {
        var listed = rules.Content
            .Split('\n')
            .Select(StripComment)
            .Where(line => line.StartsWith(Directive, StringComparison.OrdinalIgnoreCase))
            .Select(line => line[Directive.Length..].Trim())
            .Select(value => SameHostUrl(value, host))
            .OfType<string>()
            .Distinct(StringComparer.Ordinal)
            .Take(SitemapLimits.MaxFilesPerHost)
            .ToList();

        return listed.Count > 0 ? listed : [$"https://{host}/sitemap.xml"];
    }

    private static string StripComment(string line)
    {
        var hash = line.IndexOf('#', StringComparison.Ordinal);
        return (hash >= 0 ? line[..hash] : line).Trim();
    }

    private static string? SameHostUrl(string value, string host) =>
        Uri.TryCreate(value, UriKind.Absolute, out var uri) && SitemapHosts.IsSameHost(uri, host) ? uri.AbsoluteUri : null;
}
