namespace SamoBot.Core.Sitemaps;

public static class SitemapHosts
{
    /// <summary>True for an http or https URL whose authority (host and non-default port) is the crawled host.</summary>
    public static bool IsSameHost(Uri uri, string host) =>
        uri.Scheme is "http" or "https" && string.Equals(uri.Authority, host, StringComparison.OrdinalIgnoreCase);
}
