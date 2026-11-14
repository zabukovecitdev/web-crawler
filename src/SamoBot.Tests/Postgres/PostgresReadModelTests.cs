using FluentAssertions;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresReadModelTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresSiteReader Sites => new(postgres.DataSource);

    private PostgresPageReader Pages => new(postgres.DataSource);

    private static SiteQuery AllSites(SiteSort sort = SiteSort.Host, string? query = null) => new(query, sort, 50, 0);

    private static PageQuery AllPages(PageStateFilter state = PageStateFilter.All, string? query = null) => new(query, state, 50, 0);

    private async Task<(int UrlId, int FetchId)> Fetched(
        int job,
        string url,
        int status,
        long responseMs,
        string contentType = "text/html",
        string? objectName = "obj")
    {
        var urlId = await PostgresTestData.InsertUrl(postgres.DataSource, job, url);
        var fetchId = await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ContentLength", "ResponseTimeMs", "ObjectName")
            VALUES (@urlId, @at, @status, @contentType, 1000, @responseMs, @objectName) RETURNING "Id"
            """,
            new { urlId, at = TestData.Now, status, contentType, responseMs, objectName });
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetchId, "LastCrawlAt" = @at WHERE "Id" = @urlId RETURNING "Id" """,
            new { fetchId, urlId, at = TestData.Now });
        return (urlId, fetchId);
    }

    private Task<int> Parsed(int fetchId, string title, bool indexed) =>
        PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "ParsedDocuments" ("UrlFetchId", "Title", "Language", "BodyText", "Headings", "Images", "OpenGraphData", "IndexedAt")
            VALUES (@fetchId, @title, 'en', 'a b c', '[{"level":2,"text":"Sub"}]', '[{"src":"x"}]', '{"og:title":"T"}', @indexedAt)
            RETURNING "Id"
            """,
            new { fetchId, title, indexedAt = indexed ? TestData.Now : (DateTimeOffset?)null });

    private async Task SeedTwoSites()
    {
        var running = await PostgresTestData.InsertJob(postgres.DataSource, "Running");
        var paused = await PostgresTestData.InsertJob(postgres.DataSource, "Paused");
        var completed = await PostgresTestData.InsertJob(postgres.DataSource, "Completed");
        var ok = await Fetched(running, "https://a.example/ok", 200, 100);
        await Parsed(ok.FetchId, "Home page", indexed: true);
        await Fetched(running, "https://a.example/missing", 404, 300);
        await Fetched(running, "https://a.example/moved", 301, 200, objectName: null);
        await Fetched(paused, "https://b.example/doc", 200, 400, "application/pdf", null);
        await PostgresTestData.InsertUrl(postgres.DataSource, completed, "https://c.example/never");
    }

    [Fact]
    public async Task ListSitesAggregatesStatusPagesFailuresAndOverview()
    {
        await SeedTwoSites();

        var page = await Sites.ListSites(AllSites(), default);

        page.Total.Should().Be(3);
        page.Items.Select(s => s.Host).Should().Equal("a.example", "b.example", "c.example");
        var a = page.Items[0];
        a.Status.Should().Be(SiteStatus.Crawling);
        a.Pages.Should().Be(3);
        a.Failed.Should().Be(1);
        a.AvgResponseMs.Should().Be(200);
        a.ActiveJobId.Should().NotBeNull();
        a.CrawlDelayMs.Should().Be(1000);
        page.Items[1].Status.Should().Be(SiteStatus.Paused);
        page.Items[2].Status.Should().Be(SiteStatus.Never);
        page.Overview.Should().BeEquivalentTo(new SitesOverview { Sites = 3, Pages = 4, Failed = 1, Crawling = 1 }, o => o
            .Excluding(x => x.MedianResponseMs).Excluding(x => x.P95ResponseMs));
        page.Overview.MedianResponseMs.Should().BeApproximately(250, 0.001);
        page.Overview.P95ResponseMs.Should().BeApproximately(385, 0.001);
    }

    [Fact]
    public async Task ListSitesFiltersSortsAndPages()
    {
        await SeedTwoSites();

        var byFailed = await Sites.ListSites(AllSites(SiteSort.Failed), default);
        var filtered = await Sites.ListSites(AllSites(query: "B.EXA"), default);
        var literal = await Sites.ListSites(AllSites(query: "%"), default);
        var paged = await Sites.ListSites(new SiteQuery(null, SiteSort.Pages, 1, 1), default);

        byFailed.Items[0].Host.Should().Be("a.example");
        filtered.Items.Should().ContainSingle().Which.Host.Should().Be("b.example");
        literal.Total.Should().Be(0);
        paged.Total.Should().Be(3);
        paged.Items.Should().ContainSingle().Which.Host.Should().Be("b.example");
    }

    [Fact]
    public async Task FindSiteReturnsBreakdownsAndNullForAnUnknownHost()
    {
        await SeedTwoSites();

        var site = await Sites.FindSite("a.example", default);
        var missing = await Sites.FindSite("nope.example", default);

        missing.Should().BeNull();
        site!.FirstCrawlAt.Should().Be(TestData.Now);
        site.StatusCodes.Select(c => (c.StatusCode, c.Count)).Should().Equal((200, 1), (301, 1), (404, 1));
        site.ContentTypes.Should().ContainSingle().Which.Should().BeEquivalentTo(new ContentTypeCount { ContentType = "text/html", Count = 3 });
    }

    [Fact]
    public async Task ListHostPagesFiltersByStateAndTextAndDerivesTheContentState()
    {
        await SeedTwoSites();

        var all = await Pages.ListHostPages("a.example", AllPages(), default);
        var errors = await Pages.ListHostPages("a.example", AllPages(PageStateFilter.Error), default);
        var redirects = await Pages.ListHostPages("a.example", AllPages(PageStateFilter.Redirect), default);
        var byTitle = await Pages.ListHostPages("a.example", AllPages(query: "home"), default);
        var literal = await Pages.ListHostPages("a.example", AllPages(query: "_"), default);

        all.Total.Should().Be(3);
        errors.Items.Should().ContainSingle().Which.ContentState.Should().Be(ContentState.Failed);
        redirects.Items.Should().ContainSingle().Which.ContentState.Should().Be(ContentState.Redirect);
        var home = byTitle.Items.Should().ContainSingle().Subject;
        home.ContentState.Should().Be(ContentState.ParsedIndexed);
        home.Path.Should().Be("/ok");
        home.Title.Should().Be("Home page");
        literal.Total.Should().Be(0);
    }

    [Fact]
    public async Task ListHostPagesPagesTheResultAndKeepsTheTotal()
    {
        await SeedTwoSites();

        var page = await Pages.ListHostPages("a.example", new PageQuery(null, PageStateFilter.All, 2, 2), default);

        page.Total.Should().Be(3);
        page.Items.Should().HaveCount(1);
    }

    [Fact]
    public async Task FindPageReturnsFetchHistoryAndParsedFieldsNewestFetchFirst()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var (urlId, fetchId) = await Fetched(job, "https://a.example/p?x=1", 200, 120);
        await Parsed(fetchId, "Title", indexed: false);
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode") VALUES (@urlId, @older, 500) RETURNING "Id"
            """,
            new { urlId, older = TestData.Now.AddDays(-1) });

        var page = await Pages.FindPage(urlId, default);

        page!.Host.Should().Be("a.example");
        page.CrawlJobId.Should().Be(job);
        page.ContentState.Should().Be(ContentState.ParsedWaitingForIndex);
        page.Fetches.Select(f => f.StatusCode).Should().Equal(200, 500);
        page.Parsed!.Language.Should().Be("en");
        page.Parsed.WordCount.Should().Be(3);
        page.Parsed.Headings.Should().Equal(new PageHeading(2, "Sub"));
        page.Parsed.ImageCount.Should().Be(1);
        page.Parsed.OpenGraph["og:title"].Should().Be("T");
        (await Pages.FindPage(urlId + 99, default)).Should().BeNull();
    }

    [Fact]
    public async Task FindContentObjectNameReturnsTheNewestStoredBodyOrNull()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var (stored, _) = await Fetched(job, "https://a.example/s", 200, 10, objectName: "bodies/s");
        var (notStored, _) = await Fetched(job, "https://a.example/n", 200, 10, objectName: null);

        (await Pages.FindContentObjectName(stored, default)).Should().Be("bodies/s");
        (await Pages.FindContentObjectName(notStored, default)).Should().BeNull();
    }
}
