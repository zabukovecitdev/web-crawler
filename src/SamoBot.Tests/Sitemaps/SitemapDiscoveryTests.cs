using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Sitemaps;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Sitemaps;

public class SitemapDiscoveryTests
{
    private static RobotsRules Rules(string content) => new("a.example", content, TestData.Now, TestData.Now.AddDays(1));

    [Fact]
    public void SeedsReadsTheSitemapLinesOfRobotsTxtWithoutCaseOrComments()
    {
        var rules = Rules("""
            User-agent: *
            Disallow: /x
            Sitemap: https://a.example/one.xml
            sitemap:https://a.example/two.xml # main one
            SITEMAP: https://a.example/one.xml
            """);

        SitemapDiscovery.Seeds(rules, "a.example").Should().Equal("https://a.example/one.xml", "https://a.example/two.xml");
    }

    [Fact]
    public void SeedsFallsBackToSitemapXmlWhenRobotsTxtHasNoSitemapLine()
    {
        SitemapDiscovery.Seeds(Rules("User-agent: *\nDisallow:\n"), "a.example").Should().Equal("https://a.example/sitemap.xml");
    }

    [Fact]
    public void SeedsSkipsSitemapsOfOtherHostsAndFallsBackWhenNoneIsLeft()
    {
        var rules = Rules("Sitemap: https://evil.example/s.xml\nSitemap: ftp://a.example/s.xml\nSitemap: nonsense\n");

        SitemapDiscovery.Seeds(rules, "a.example").Should().Equal("https://a.example/sitemap.xml");
    }

    [Fact]
    public void SeedsCapsTheListAtTheFileLimit()
    {
        var lines = string.Join('\n', Enumerable.Range(0, 80).Select(i => $"Sitemap: https://a.example/{i}.xml"));

        SitemapDiscovery.Seeds(Rules(lines), "a.example").Should().HaveCount(SitemapLimits.MaxFilesPerHost);
    }
}
