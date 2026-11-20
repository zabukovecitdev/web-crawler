using System.Globalization;
using System.IO.Compression;
using System.Text;
using System.Xml;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Sitemaps;

public sealed record SitemapParseResult(SitemapKind Kind, IReadOnlyList<SitemapEntry> Entries);

public static class SitemapParser
{
    public static SitemapParseResult? Parse(byte[] raw, string host)
    {
        if (raw.Length == 0 || raw.Length > SitemapLimits.MaxCompressedBytes)
        {
            return null;
        }

        return ParseXml(raw, host);
    }

    private static bool IsGzip(byte[] raw) => raw.Length > 2 && raw[0] == 0x1f && raw[1] == 0x8b;

    private static Stream Open(byte[] raw) => IsGzip(raw)
        ? new CappedReadStream(new GZipStream(new MemoryStream(raw), CompressionMode.Decompress), SitemapLimits.MaxDecompressedBytes)
        : new MemoryStream(raw);

    private static SitemapParseResult? ParseXml(byte[] raw, string host)
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
            using var source = Open(raw);
            using var reader = XmlReader.Create(source, settings);
            return ReadDocument(reader, host);
        }
        catch (Exception ex) when (ex is XmlException or InvalidDataException)
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
