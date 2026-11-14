using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

/// <summary>UNVERIFIED in the authoring environment: written without Docker, so these tests have never run.</summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresSitemapTests(PostgresFixture postgres) : IAsyncLifetime
{
    private const string Host = "a.example";

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public async Task InitializeAsync()
    {
        await postgres.Reset();
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """WITH a AS (DELETE FROM "SitemapUrls"), b AS (DELETE FROM "SitemapFiles"), c AS (DELETE FROM "SitemapRequests") SELECT 1""");
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresSitemapStore Store => new(postgres.DataSource, _time);

    private PostgresSitemapReader Reader => new(postgres.DataSource);

    private static SitemapEntry Entry(string path) => new($"https://a.example/{path}", $"https://a.example/{path}", null);

    private static SitemapFileContent File(string url, string? parent, SitemapKind kind, params SitemapEntry[] urls) =>
        new(new SitemapFile(Host, url, parent, kind, urls.Length, TestData.Now), urls);

    private async Task Fetched(int job, string path, int status)
    {
        var urlId = await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://a.example/{path}");
        var fetchId = await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode") VALUES (@urlId, @at, @status) RETURNING "Id"
            """,
            new { urlId, at = TestData.Now, status });
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetchId, "LastCrawlAt" = @at WHERE "Id" = @urlId RETURNING "Id" """,
            new { fetchId, urlId, at = TestData.Now });
    }

    [Fact]
    public async Task CompleteStoresFilesAndUrlsAndTheReaderPagesThem()
    {
        await Store.Complete(Host, [
            File("https://a.example/sitemap.xml", null, SitemapKind.Index),
            File("https://a.example/s1.xml", "https://a.example/sitemap.xml", SitemapKind.UrlSet, Entry("b"), Entry("a"), Entry("c"))], default);

        var snapshot = await Reader.Read(Host, 2, 1, default);

        snapshot.Files.Select(f => (f.Url, f.Kind, f.ParentUrl)).Should().Equal(
            ("https://a.example/sitemap.xml", SitemapKind.Index, null),
            ("https://a.example/s1.xml", SitemapKind.UrlSet, "https://a.example/sitemap.xml"));
        snapshot.Counts.UrlCount.Should().Be(3);
        snapshot.Urls.Total.Should().Be(3);
        snapshot.Urls.Items.Select(u => u.Loc).Should().Equal("https://a.example/b", "https://a.example/c");
    }

    [Fact]
    public async Task CompleteReplacesTheOldFilesButAnEmptyListKeepsThem()
    {
        await Store.Complete(Host, [File("https://a.example/old.xml", null, SitemapKind.UrlSet, Entry("old"))], default);
        await Store.Complete(Host, [], default);
        (await Reader.Read(Host, 50, 0, default)).Urls.Items.Select(u => u.Loc).Should().Equal("https://a.example/old");

        await Store.Complete(Host, [File("https://a.example/new.xml", null, SitemapKind.UrlSet, Entry("new"))], default);

        var snapshot = await Reader.Read(Host, 50, 0, default);
        snapshot.Files.Select(f => f.Url).Should().Equal("https://a.example/new.xml");
        snapshot.Urls.Items.Select(u => u.Loc).Should().Equal("https://a.example/new");
    }

    [Fact]
    public async Task ReadOfAHostWithoutSitemapsIsEmpty()
    {
        var snapshot = await Reader.Read("never.example", 50, 0, default);

        snapshot.Files.Should().BeEmpty();
        snapshot.Counts.Should().Be(new SitemapCounts(0, 0, 0));
        snapshot.Urls.Items.Should().BeEmpty();
    }

    [Fact]
    public async Task ReadJoinsTheCrawledPagesAndCountsThePagesMissingFromTheSitemap()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, "Completed");
        await Fetched(job, "a", 200);
        await Fetched(job, "extra", 404);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/queued");
        await Store.Complete(Host, [File("https://a.example/s.xml", null, SitemapKind.UrlSet, Entry("a"), Entry("queued"), Entry("unknown"))], default);

        var snapshot = await Reader.Read(Host, 50, 0, default);

        snapshot.Counts.Should().Be(new SitemapCounts(3, 1, 1));
        var rows = snapshot.Urls.Items.ToDictionary(u => u.Loc);
        rows["https://a.example/a"].Should().Match<SitemapUrlRow>(r => r.StatusCode == 200 && r.PageId != null);
        rows["https://a.example/queued"].PageId.Should().BeNull();
        rows["https://a.example/unknown"].StatusCode.Should().BeNull();
    }

    [Fact]
    public async Task ClaimNextGivesTheOldestQueuedHostOnceUntilTheLeaseEnds()
    {
        await Store.Enqueue("b.example", default);
        _time.Advance(TimeSpan.FromSeconds(1));
        await Store.Enqueue("c.example", default);

        var first = await Store.ClaimNext(TimeSpan.FromMinutes(5), default);
        var second = await Store.ClaimNext(TimeSpan.FromMinutes(5), default);
        var none = await Store.ClaimNext(TimeSpan.FromMinutes(5), default);
        _time.Advance(TimeSpan.FromMinutes(6));
        var again = await Store.ClaimNext(TimeSpan.FromMinutes(5), default);

        (first, second, none, again).Should().Be(("b.example", "c.example", null, "b.example"));
    }

    [Fact]
    public async Task CompleteFinishesTheQueueRowSoItIsNotClaimedAgain()
    {
        await Store.Enqueue(Host, default);
        (await Store.ClaimNext(TimeSpan.FromMinutes(5), default)).Should().Be(Host);

        await Store.Complete(Host, [], default);
        _time.Advance(TimeSpan.FromHours(1));

        (await Store.ClaimNext(TimeSpan.FromMinutes(5), default)).Should().BeNull();
    }

    [Fact]
    public async Task EnqueueFinishedHostsQueuesOnlyHostsWithACompletedJobAndRefreshesStaleOnes()
    {
        var completed = await PostgresTestData.InsertJob(postgres.DataSource, "Completed");
        var running = await PostgresTestData.InsertJob(postgres.DataSource, "Running");
        await PostgresTestData.InsertUrl(postgres.DataSource, completed, "https://a.example/x");
        await PostgresTestData.InsertUrl(postgres.DataSource, running, "https://b.example/x");

        await Store.EnqueueFinishedHosts(TimeSpan.FromDays(7), default);
        (await Store.ClaimNext(TimeSpan.FromMinutes(5), default)).Should().Be(Host);
        (await Store.ClaimNext(TimeSpan.FromMinutes(5), default)).Should().BeNull();
        await Store.Complete(Host, [], default);

        await Store.EnqueueFinishedHosts(TimeSpan.FromDays(7), default);
        (await Store.ClaimNext(TimeSpan.FromMinutes(5), default)).Should().BeNull();
        _time.Advance(TimeSpan.FromDays(8));
        await Store.EnqueueFinishedHosts(TimeSpan.FromDays(7), default);
        (await Store.ClaimNext(TimeSpan.FromMinutes(5), default)).Should().Be(Host);
    }

    [Fact]
    public async Task ReserveFetchSlotSpacesTheFetchesByTheDelayOfTheHost()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/x");
        await PostgresTestData.Scalar<int>(
            postgres.DataSource, """UPDATE "Hosts" SET "CrawlDelayMs" = 2000 WHERE "Host" = 'a.example' RETURNING 1""");

        var first = await Store.ReserveFetchSlot(Host, default);
        var second = await Store.ReserveFetchSlot(Host, default);
        var unknownHost = await Store.ReserveFetchSlot("nohost.example", default);

        first.Should().BeLessThanOrEqualTo(TimeSpan.Zero);
        second.Should().BeCloseTo(TimeSpan.FromSeconds(2), TimeSpan.FromMilliseconds(50));
        unknownHost.Should().Be(TimeSpan.Zero);
    }
}
