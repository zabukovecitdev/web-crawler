using System.Text;
using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Sitemaps;

namespace SamoBot.Tests.Sitemaps;

public class SitemapParserTests
{
    private const string Host = "a.example";

    private static SitemapParseResult? Parse(string xml) => SitemapParser.Parse(Encoding.UTF8.GetBytes(xml), Host);

    [Fact]
    public void ParseReadsTheLocationsAndLastModifiedDatesOfAUrlSet()
    {
        var xml = SitemapXml.UrlSet([("https://a.example/one", "2024-05-01"), ("https://a.example/two", null)]);

        var result = Parse(xml)!;

        result.Kind.Should().Be(SitemapKind.UrlSet);
        result.Entries.Select(e => e.Loc).Should().Equal("https://a.example/one", "https://a.example/two");
        result.Entries[0].LastModified.Should().Be(new DateTimeOffset(2024, 5, 1, 0, 0, 0, TimeSpan.Zero));
        result.Entries[1].LastModified.Should().BeNull();
    }

    [Fact]
    public void ParseReadsTheChildrenOfASitemapIndex()
    {
        var result = Parse(SitemapXml.Index("https://a.example/s1.xml", "https://a.example/s2.xml.gz"))!;

        result.Kind.Should().Be(SitemapKind.Index);
        result.Entries.Select(e => e.Loc).Should().Equal("https://a.example/s1.xml", "https://a.example/s2.xml.gz");
    }

    [Fact]
    public void ParseReadsAGzipFile()
    {
        var result = SitemapParser.Parse(SitemapXml.Gzip(SitemapXml.UrlSet("https://a.example/z")), Host)!;

        result.Entries.Should().ContainSingle().Which.Loc.Should().Be("https://a.example/z");
    }

    [Fact]
    public void ParseNormalizesTheLocationLikeTheCrawlerDoes()
    {
        var result = Parse(SitemapXml.UrlSet("https://A.example:443/p?b=2&amp;a=1#frag"))!;

        result.Entries.Single().NormalizedLoc.Should().Be("https://a.example/p?a=1&b=2");
    }

    [Fact]
    public void ParseRejectsAGzipBombThatExpandsPastTheLimit()
    {
        var bomb = new byte[SitemapLimits.MaxDecompressedBytes + 1024];
        Array.Fill(bomb, (byte)' ');
        var compressed = SitemapXml.Gzip(bomb);

        compressed.Length.Should().BeLessThan(SitemapLimits.MaxCompressedBytes);
        SitemapParser.Parse(compressed, Host).Should().BeNull();
    }

    [Fact]
    public void ParseRejectsAFileOverTheCompressedLimit()
    {
        SitemapParser.Parse(new byte[SitemapLimits.MaxCompressedBytes + 1], Host).Should().BeNull();
    }

    [Fact]
    public void ParseRefusesADtdSoAnExternalEntityIsNeverRead()
    {
        const string xxe = """
            <?xml version="1.0"?>
            <!DOCTYPE urlset [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://a.example/&xxe;</loc></url></urlset>
            """;

        Parse(xxe).Should().BeNull();
    }

    [Fact]
    public void ParseRefusesAnEntityExpansionBomb()
    {
        const string billion = """
            <?xml version="1.0"?>
            <!DOCTYPE urlset [<!ENTITY a "aaaaaaaaaa"><!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">]>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://a.example/&b;</loc></url></urlset>
            """;

        Parse(billion).Should().BeNull();
    }

    [Fact]
    public void ParseKeepsOnlyHttpUrlsOfTheSameHost()
    {
        var xml = SitemapXml.UrlSet(
            "https://a.example/ok",
            "http://a.example/also-ok",
            "https://evil.example/foreign",
            "https://a.example:8443/other-port",
            "ftp://a.example/file",
            "javascript:alert(1)",
            "/relative");

        var result = Parse(xml)!;

        result.Entries.Select(e => e.Loc).Should().Equal("https://a.example/ok", "http://a.example/also-ok");
    }

    [Fact]
    public void ParseDropsChildSitemapsOnAnotherHost()
    {
        var result = Parse(SitemapXml.Index("https://evil.example/s.xml", "https://a.example/s.xml"))!;

        result.Entries.Select(e => e.Loc).Should().Equal("https://a.example/s.xml");
    }

    [Fact]
    public void ParseDropsALocationOver2048Bytes()
    {
        var tooLong = "https://a.example/" + new string('x', 2048);
        var justFits = "https://a.example/" + new string('y', 2048 - "https://a.example/".Length);

        var result = Parse(SitemapXml.UrlSet(tooLong, justFits))!;

        result.Entries.Should().ContainSingle().Which.Loc.Should().Be(justFits);
    }

    [Fact]
    public void ParseKeepsAtMostFiftyThousandUrlsOfAFile()
    {
        var locs = Enumerable.Range(0, SitemapLimits.MaxUrlsPerFile + 10).Select(i => ($"https://a.example/{i}", (string?)null));

        var result = Parse(SitemapXml.UrlSet(locs))!;

        result.Entries.Should().HaveCount(SitemapLimits.MaxUrlsPerFile);
    }

    [Theory]
    [InlineData("")]
    [InlineData("not xml at all")]
    [InlineData("<html><body>nope</body></html>")]
    [InlineData("<urlset><url><loc>https://a.example/x</loc></url>")]
    public void ParseGivesNullForInputThatIsNotASitemap(string text)
    {
        Parse(text).Should().BeNull();
    }

    [Fact]
    public void ParseGivesNullForBrokenGzip()
    {
        SitemapParser.Parse([0x1f, 0x8b, 0x08, 0x00, 0x01, 0x02, 0x03], Host).Should().BeNull();
    }

    [Fact]
    public void ParseIgnoresAnUnparsableLastModified()
    {
        var result = Parse(SitemapXml.UrlSet([("https://a.example/x", "yesterday")]))!;

        result.Entries.Single().LastModified.Should().BeNull();
    }
}
