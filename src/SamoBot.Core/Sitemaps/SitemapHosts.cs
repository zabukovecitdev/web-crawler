namespace SamoBot.Core.Sitemaps;

public static class SitemapHosts
{
    public static bool IsSameHost(Uri uri, string host) =>
        uri.Scheme is "http" or "https" && string.Equals(uri.Authority, host, StringComparison.OrdinalIgnoreCase);
}
