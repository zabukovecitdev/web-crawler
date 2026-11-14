using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Xml;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Sitemaps;

public sealed record SitemapParseResult(SitemapKind Kind, IReadOnlyList<SitemapEntry> Entries);

/// <summary>
/// Parses a urlset or a sitemapindex, plain or gzip. Bad input gives null. DTDs are prohibited and no resolver is set (XXE safe).
/// Only http and https URLs of the same host stay. A file keeps at most 50,000 entries.
/// </summary>
public static class SitemapParser
{
    public static SitemapParseResult? Parse(byte[] raw, string host)
    {
        if (raw.Length == 0 || raw.Length > SitemapLimits.MaxCompressedBytes)
        {
            return null;
        }

        var xml = IsGzip(raw) ? Decompress(raw) : raw;
        return xml is null ? null : ParseXml(xml, host);
    }

    private static bool IsGzip(byte[] raw) => raw.Length > 2 && raw[0] == 0x1f && raw[1] == 0x8b;

    /// <summary>Reads at most 50 MB. A larger stream (a gzip bomb) gives null.</summary>
    private static byte[]? Decompress(byte[] raw)
    {
        try
        {
            using var gzip = new GZipStream(new MemoryStream(raw), CompressionMode.Decompress);
            using var output = new MemoryStream();
            return CopyCapped(gzip, output) ? output.ToArray() : null;
        }
        catch (InvalidDataException)
        {
            return null;
        }
    }

    private static bool CopyCapped(Stream source, MemoryStream output)
    {
        var chunk = new byte[81920];
        int read;
        while ((read = source.Read(chunk, 0, chunk.Length)) > 0 && output.Length <= SitemapLimits.MaxDecompressedBytes)
        {
            output.Write(chunk, 0, read);
        }

        return output.Length <= SitemapLimits.MaxDecompressedBytes;
    }

    private static SitemapParseResult? ParseXml(byte[] xml, string host)
    {
        var settings = new XmlReaderSettings
        {
            DtdProcessing = DtdProcessing.Prohibit,
            XmlResolver = null,
            MaxCharactersInDocument = SitemapLimits.MaxDecompressedBytes,
            IgnoreComments = true,
            IgnoreProcessingInstructions = true
        };

        try
        {
            using var reader = XmlReader.Create(new MemoryStream(xml), settings);
            return ReadDocument(reader, host);
        }
        catch (XmlException)
        {
            return null;
        }
    }

    private static SitemapParseResult? ReadDocument(XmlReader reader, string host)
    {
        if (reader.MoveToContent() != XmlNodeType.Element)
        {
            return null;
        }

        return reader.LocalName switch
        {
            "urlset" => new SitemapParseResult(SitemapKind.UrlSet, ReadEntries(reader, "url", host)),
            "sitemapindex" => new SitemapParseResult(SitemapKind.Index, ReadEntries(reader, "sitemap", host)),
            _ => null
        };
    }

    private static List<SitemapEntry> ReadEntries(XmlReader reader, string entryName, string host)
    {
        var entries = new List<SitemapEntry>();
        while (entries.Count < SitemapLimits.MaxUrlsPerFile && reader.Read())
        {
            if (reader.NodeType != XmlNodeType.Element || reader.LocalName != entryName)
            {
                continue;
            }

            using var entry = reader.ReadSubtree();
            if (ReadEntry(entry, host) is { } parsed)
            {
                entries.Add(parsed);
            }
        }

        return entries;
    }

    private static SitemapEntry? ReadEntry(XmlReader entry, string host)
    {
        string? loc = null;
        string? lastModified = null;
        entry.Read();
        while (!entry.EOF)
        {
            var isChild = entry.NodeType == XmlNodeType.Element && entry.Depth == 1;
            if (isChild && entry.LocalName == "loc")
            {
                loc = entry.ReadElementContentAsString();
            }
            else if (isChild && entry.LocalName == "lastmod")
            {
                lastModified = entry.ReadElementContentAsString();
            }
            else
            {
                entry.Read();
            }
        }

        return Accept(loc, lastModified, host);
    }

    private static SitemapEntry? Accept(string? loc, string? lastModified, string host)
    {
        var trimmed = loc?.Trim();
        if (string.IsNullOrEmpty(trimmed) || TooLong(trimmed) || !Uri.TryCreate(trimmed, UriKind.Absolute, out var uri))
        {
            return null;
        }

        var normalized = Normalize(uri);
        return SitemapHosts.IsSameHost(uri, host) && !TooLong(normalized)
            ? new SitemapEntry(trimmed, normalized, ParseDate(lastModified))
            : null;
    }

    private static string Normalize(Uri uri) =>
        UrlNormalizer.TryClean(uri.AbsoluteUri, out var clean) && clean is not null ? clean.AbsoluteUri : uri.AbsoluteUri;

    private static bool TooLong(string text) => Encoding.UTF8.GetByteCount(text) > SitemapLimits.MaxLocBytes;

    private static DateTimeOffset? ParseDate(string? text) =>
        DateTimeOffset.TryParse(text?.Trim(), CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var date) ? date : null;
}
