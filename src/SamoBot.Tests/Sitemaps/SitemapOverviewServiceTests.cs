using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Api.Contracts;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Sitemaps;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Sitemaps;

public class SitemapOverviewServiceTests
{
    private const string Host = "a.example";

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private static SitemapOverviewService Build(SitemapSnapshot snapshot, string robots)
    {
        var rules = new RobotsRules(Host, robots, TestData.Now, TestData.Now.AddDays(1));
        var gate = new RobotsGate(
            new MemoryCache(Options.Create(new MemoryCacheOptions())),
            new FakeRobotsStore(),
            new FakeRobotsSource(rules),
            new FakeTimeProvider(TestData.Now));
        return new SitemapOverviewService(new FakeSitemapReader(snapshot), gate);
    }

    private static SitemapFile File() => new(Host, "https://a.example/sitemap.xml", null, SitemapKind.UrlSet, 3, TestData.Now);

    private static SitemapSnapshot Snapshot(params SitemapUrlRow[] rows) =>
        new([File()], new SitemapCounts(3, 1, 4), new PagedResult<SitemapUrlRow>(3, rows));

    [Fact]
    public async Task ReadMarksFetchedUrlsCrawledAndDisallowedUnfetchedUrlsBlocked()
    {
        var snapshot = Snapshot(
            new SitemapUrlRow { Loc = "https://a.example/done", StatusCode = 200, PageId = 5 },
            new SitemapUrlRow { Loc = "https://a.example/todo" },
            new SitemapUrlRow { Loc = "https://a.example/private/x" });

        var overview = await Build(snapshot, "User-agent: *\nDisallow: /private\n").Read(Host, 50, 0, default);

        overview.Urls.Items.Select(i => i.CrawlState).Should().Equal(
            SitemapCrawlState.Crawled, SitemapCrawlState.NotCrawled, SitemapCrawlState.Blocked);
        overview.Urls.Items[0].Should().Match<SitemapUrlItem>(i => i.StatusCode == 200 && i.PageId == 5);
        overview.Urls.Total.Should().Be(3);
    }

    [Fact]
    public async Task ReadKeepsACrawledUrlCrawledEvenWhenRobotsTxtNowDisallowsIt()
    {
        var snapshot = Snapshot(new SitemapUrlRow { Loc = "https://a.example/private/x", StatusCode = 200, PageId = 9 });

        var overview = await Build(snapshot, "User-agent: *\nDisallow: /private\n").Read(Host, 50, 0, default);

        overview.Urls.Items.Single().CrawlState.Should().Be(SitemapCrawlState.Crawled);
    }

    [Fact]
    public async Task ReadOfAHostWithoutSitemapsGivesAnEmptyOverview()
    {
        var empty = new SitemapSnapshot([], new SitemapCounts(0, 0, 0), new PagedResult<SitemapUrlRow>(0, []));

        var overview = await Build(empty, "User-agent: *\nDisallow:\n").Read(Host, 50, 0, default);

        overview.Files.Should().BeEmpty();
        overview.Urls.Items.Should().BeEmpty();
        overview.Counts.Should().Be(new SitemapCounts(0, 0, 0));
    }

    [Fact]
    public async Task SitemapDtoHasTheShapeOfTheFrontendContract()
    {
        var snapshot = Snapshot(new SitemapUrlRow { Loc = "https://a.example/done", StatusCode = 200, PageId = 5 });
        var overview = await Build(snapshot, "User-agent: *\nDisallow:\n").Read(Host, 50, 0, default);

        var json = JsonSerializer.SerializeToElement(SitemapDto.From(overview), Web);

        json.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("host", "files", "urlCount", "crawledCount", "notInSitemapCount", "urls");
        json.GetProperty("files")[0].EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("url", "parentUrl", "kind", "urlCount", "fetchedAt");
        json.GetProperty("files")[0].GetProperty("kind").GetString().Should().Be("UrlSet");
        var item = json.GetProperty("urls").GetProperty("items")[0];
        item.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("loc", "lastModified", "crawlState", "statusCode", "pageId");
        item.GetProperty("crawlState").GetString().Should().Be("Crawled");
    }
}
