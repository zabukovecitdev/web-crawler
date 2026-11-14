using System.Globalization;
using System.IO.Compression;
using System.Text;

namespace SamoBot.Tests.Sitemaps;

/// <summary>Builds sitemap documents for the tests.</summary>
public static class SitemapXml
{
    private const string Namespace = "http://www.sitemaps.org/schemas/sitemap/0.9";

    public static string UrlSet(params string[] locs) => UrlSet(locs.Select(loc => (loc, (string?)null)));

    public static string UrlSet(IEnumerable<(string Loc, string? LastMod)> entries)
    {
        var body = new StringBuilder();
        foreach (var (loc, lastMod) in entries)
        {
            var mod = lastMod is null ? string.Empty : $"<lastmod>{lastMod}</lastmod>";
            body.Append(CultureInfo.InvariantCulture, $"<url><loc>{loc}</loc>{mod}</url>");
        }

        return $"""<?xml version="1.0" encoding="UTF-8"?><urlset xmlns="{Namespace}">{body}</urlset>""";
    }

    public static string Index(params string[] locs)
    {
        var body = string.Concat(locs.Select(loc => $"<sitemap><loc>{loc}</loc></sitemap>"));
        return $"""<?xml version="1.0" encoding="UTF-8"?><sitemapindex xmlns="{Namespace}">{body}</sitemapindex>""";
    }

    public static byte[] Gzip(byte[] data)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionLevel.SmallestSize))
        {
            gzip.Write(data);
        }

        return output.ToArray();
    }

    public static byte[] Gzip(string text) => Gzip(Encoding.UTF8.GetBytes(text));
}
