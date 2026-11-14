using FluentAssertions;
using Microsoft.Extensions.Caching.Memory;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Sitemaps;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Sitemaps;

public class SitemapRefresherTests
{
    private const string Host = "a.example";

    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly FakeSitemapSource _source = new();
    private readonly FakeSitemapStore _store = new();

    private SitemapRefresher Build(string robots = "User-agent: *\nDisallow:\n")
    {
        var rules = new RobotsRules(Host, robots, TestData.Now, TestData.Now.AddDays(1));
        var gate = new RobotsGate(new MemoryCache(Options.Create(new MemoryCacheOptions())), new FakeRobotsStore(), new FakeRobotsSource(rules), _time);
        return new SitemapRefresher(gate, _source, _store, _time);
    }

    private IReadOnlyList<SitemapFileContent> Stored => _store.Completed.Single().Files;

    [Fact]
    public async Task RefreshFollowsTheSitemapLineOfRobotsTxtAndStoresTheUrls()
    {
        _source.Serve("https://a.example/map.xml", SitemapXml.UrlSet("https://a.example/1", "https://a.example/2"));

        var count = await Build("Sitemap: https://a.example/map.xml\n").Refresh(Host, default);

        count.Should().Be(1);
        Stored.Single().File.Should().Match<SitemapFile>(f => f.Kind == SitemapKind.UrlSet && f.UrlCount == 2 && f.ParentUrl == null && f.FetchedAt == TestData.Now);
        Stored.Single().Urls.Select(u => u.Loc).Should().Equal("https://a.example/1", "https://a.example/2");
    }

    [Fact]
    public async Task RefreshUsesSitemapXmlWhenRobotsTxtListsNone()
    {
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.UrlSet("https://a.example/1"));

        await Build().Refresh(Host, default);

        _source.Downloaded.Should().Equal("https://a.example/sitemap.xml");
    }

    [Fact]
    public async Task RefreshReadsTheChildrenOfAnIndexIncludingAGzipChild()
    {
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.Index("https://a.example/a.xml", "https://a.example/b.xml.gz"))
            .Serve("https://a.example/a.xml", SitemapXml.UrlSet("https://a.example/1"))
            .Serve("https://a.example/b.xml.gz", SitemapXml.Gzip(SitemapXml.UrlSet("https://a.example/2")));

        await Build().Refresh(Host, default);

        Stored.Select(f => (f.File.Url, f.File.Kind, f.File.ParentUrl)).Should().Equal(
            ("https://a.example/sitemap.xml", SitemapKind.Index, null),
            ("https://a.example/a.xml", SitemapKind.UrlSet, "https://a.example/sitemap.xml"),
            ("https://a.example/b.xml.gz", SitemapKind.UrlSet, "https://a.example/sitemap.xml"));
    }

    [Fact]
    public async Task RefreshNeverFetchesAChildOnAnotherHost()
    {
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.Index("https://evil.example/x.xml", "https://a.example/a.xml"))
            .Serve("https://a.example/a.xml", SitemapXml.UrlSet("https://a.example/1"));

        await Build().Refresh(Host, default);

        _source.Downloaded.Should().NotContain(url => url.Contains("evil"));
    }

    [Fact]
    public async Task RefreshStopsAtTwoLevelsOfIndexes()
    {
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.Index("https://a.example/l1.xml"))
            .Serve("https://a.example/l1.xml", SitemapXml.Index("https://a.example/l2.xml"))
            .Serve("https://a.example/l2.xml", SitemapXml.Index("https://a.example/l3.xml"))
            .Serve("https://a.example/l3.xml", SitemapXml.UrlSet("https://a.example/deep"));

        await Build().Refresh(Host, default);

        _source.Downloaded.Should().Equal("https://a.example/sitemap.xml", "https://a.example/l1.xml", "https://a.example/l2.xml");
    }

    [Fact]
    public async Task RefreshReadsAtMostFiftyFilesAndIgnoresALoop()
    {
        var children = Enumerable.Range(0, 80).Select(i => $"https://a.example/c{i}.xml").Append("https://a.example/sitemap.xml").ToArray();
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.Index(children));
        foreach (var child in children[..80])
        {
            _source.Serve(child, SitemapXml.UrlSet(child.Replace(".xml", "/p", StringComparison.Ordinal)));
        }

        await Build().Refresh(Host, default);

        Stored.Should().HaveCount(SitemapLimits.MaxFilesPerHost);
        _source.Downloaded.Distinct().Should().HaveCount(_source.Downloaded.Count);
    }

    [Fact]
    public async Task RefreshStoresAUrlOnlyOnceWhenTwoFilesListIt()
    {
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.Index("https://a.example/a.xml", "https://a.example/b.xml"))
            .Serve("https://a.example/a.xml", SitemapXml.UrlSet("https://a.example/same", "https://a.example/a-only"))
            .Serve("https://a.example/b.xml", SitemapXml.UrlSet("https://a.example/same", "https://a.example/b-only"));

        await Build().Refresh(Host, default);

        Stored.SelectMany(f => f.Urls).Select(u => u.Loc).Should().OnlyHaveUniqueItems().And.HaveCount(3);
        Stored[2].File.UrlCount.Should().Be(1);
    }

    [Fact]
    public async Task RefreshRespectsRobotsTxtAndFetchesNothingThatIsDisallowed()
    {
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.UrlSet("https://a.example/1"));

        await Build("User-agent: *\nDisallow: /sitemap\n").Refresh(Host, default);

        _source.Downloaded.Should().BeEmpty();
    }

    [Fact]
    public async Task RefreshBooksAFetchSlotBeforeEveryDownloadAndWaitsForIt()
    {
        _store.Wait = TimeSpan.FromSeconds(5);
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.UrlSet("https://a.example/1"));

        var refresh = Build().Refresh(Host, default);
        await Task.Yield();
        _source.Downloaded.Should().BeEmpty();
        _time.Advance(TimeSpan.FromSeconds(5));
        await refresh;

        _store.Reservations.Should().Be(1);
        _source.Downloaded.Should().HaveCount(1);
    }

    [Fact]
    public async Task RefreshWithNoUsableFileCompletesWithAnEmptyListSoOldDataStays()
    {
        _source.Serve("https://a.example/sitemap.xml", "<html>not a sitemap</html>");

        var count = await Build().Refresh(Host, default);

        count.Should().Be(0);
        _store.Completed.Single().Files.Should().BeEmpty();
    }

    [Fact]
    public async Task BatchHandlerQueuesFinishedHostsClaimsOneAndRefreshesIt()
    {
        _store.Claims.Enqueue(Host);
        _source.Serve("https://a.example/sitemap.xml", SitemapXml.UrlSet("https://a.example/1"));
        var handler = new SitemapBatchHandler(_store, Build());

        var first = await handler.HandleBatch(default);
        var second = await handler.HandleBatch(default);

        (first, second).Should().Be((1, 0));
        _store.EnqueueFinishedCalls.Should().Be(2);
        _store.Completed.Should().ContainSingle().Which.Host.Should().Be(Host);
    }
}
