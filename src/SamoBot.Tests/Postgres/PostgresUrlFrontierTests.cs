using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresUrlFrontierTests(PostgresFixture postgres) : IAsyncLifetime
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(1);

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresUrlFrontier Frontier() => new(postgres.DataSource, _time);

    [Fact]
    public async Task ClaimParallelCallsReturnNoCommonUrl()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, allowedHosts: "[]");
        for (var i = 0; i < 12; i++)
        {
            await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://h{i}.example/");
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Frontier().Claim(12, Lease, default)));

        var ids = results.SelectMany(r => r).Select(u => u.Id).ToList();
        ids.Should().OnlyHaveUniqueItems();
        ids.Should().NotBeEmpty();
    }

    [Fact]
    public async Task ClaimGivesOneUrlPerHostAndThenWaitsForTheHostDelay()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/deep", depth: 2);
        var first = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/", depth: 0);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/mid", depth: 1);

        var claimed = await Frontier().Claim(10, Lease, default);
        var immediately = await Frontier().Claim(10, Lease, default);
        _time.Advance(TimeSpan.FromSeconds(2));
        var afterDelay = await Frontier().Claim(10, Lease, default);

        claimed.Select(u => u.Id).Should().Equal(first);
        immediately.Should().BeEmpty();
        afterDelay.Should().ContainSingle().Which.Url.Should().Be("https://a.example/mid");
    }

    [Fact]
    public async Task ClaimDoesNotReturnUrlsOfAPausedJob()
    {
        var paused = await PostgresTestData.InsertJob(postgres.DataSource, status: "Paused");
        await PostgresTestData.InsertUrl(postgres.DataSource, paused, "https://a.example/");

        (await Frontier().Claim(10, Lease, default)).Should().BeEmpty();
    }

    [Fact]
    public async Task ClaimReturnsTheUrlAgainAfterTheLeaseExpires()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var id = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");

        var first = await Frontier().Claim(10, Lease, default);
        _time.Advance(TimeSpan.FromSeconds(30));
        var whileLeased = await Frontier().Claim(10, Lease, default);
        _time.Advance(TimeSpan.FromMinutes(2));
        var afterExpiry = await Frontier().Claim(10, Lease, default);

        first.Should().ContainSingle().Which.Id.Should().Be(id);
        whileLeased.Should().BeEmpty();
        afterExpiry.Should().ContainSingle().Which.Id.Should().Be(id);
    }

    [Fact]
    public async Task ClaimIsNotBlockedByHostsWithoutDueUrls()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, allowedHosts: "[]");
        for (var i = 0; i < 25; i++)
        {
            await PostgresTestData.InsertUrl(
                postgres.DataSource, job, $"https://later{i}.example/", nextCrawlAt: TestData.Now.AddDays(1));
        }

        var due = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://due.example/");

        var claimed = await Frontier().Claim(5, Lease, default);

        claimed.Should().ContainSingle().Which.Id.Should().Be(due);
    }

    /// <summary>UNVERIFIED: written without Docker.</summary>
    [Fact]
    public async Task ClaimReturnsTheCrawlDelayOfTheHost()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");
        await PostgresTestData.Scalar<int>(
            postgres.DataSource, """UPDATE "Hosts" SET "CrawlDelayMs" = 2500 WHERE "Host" = 'a.example' RETURNING 1""");

        var claimed = await Frontier().Claim(1, Lease, default);

        claimed.Should().ContainSingle().Which.CrawlDelayMs.Should().Be(2500);
    }

    private async Task<ClaimedUrl> ClaimOne(string url, int depth = 0)
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, url, depth);
        return (await Frontier().Claim(1, Lease, default)).Single();
    }

    private static FetchRecord Fetch() => new()
    {
        FetchedAt = TestData.Now,
        StatusCode = 200,
        ContentType = "text/html",
        ContentLength = 13,
        ResponseTimeMs = 42,
        ObjectName = "obj-1",
        FinalUrl = "https://a.example/"
    };

    [Fact]
    public async Task CompleteStoresFetchAndUpdatesTheUrlInOneStep()
    {
        var url = await ClaimOne("https://a.example/");
        var update = new Succeeded(TimeSpan.FromDays(1)).ToUpdate(url, TestData.Now);

        await Frontier().Complete(url, update, Fetch(), default);

        var row = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "DiscoveredUrls" WHERE "Id" = @id""", new { id = url.Id });
        ((string)row.Status).Should().Be("Idle");
        ((DateTimeOffset)row.NextCrawlAt).Should().Be(TestData.Now.AddDays(1));
        ((DateTimeOffset)row.LastCrawlAt).Should().Be(TestData.Now);
        ((object?)row.LeaseUntil).Should().BeNull();
        var fetch = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "UrlFetches" WHERE "DiscoveredUrlId" = @id""", new { id = url.Id });
        ((int)row.LastFetchId).Should().Be((int)fetch.Id);
        ((string)fetch.ObjectName).Should().Be("obj-1");
    }

    [Fact]
    public async Task CompleteOfADisabledUrlWithoutFetchStoresReasonAndNoNextCrawl()
    {
        var url = await ClaimOne("https://a.example/");

        await Frontier().Complete(url, new Disabled("robots").ToUpdate(url, TestData.Now), null, default);

        var row = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "DiscoveredUrls" WHERE "Id" = @id""", new { id = url.Id });
        ((string)row.Status).Should().Be("Disabled");
        ((string)row.FailReason).Should().Be("robots");
        ((object?)row.NextCrawlAt).Should().BeNull();
        ((object?)row.LastCrawlAt).Should().BeNull();
    }

    [Fact]
    public async Task CompleteOfARedirectAddsTheTargetAtTheSameDepthWithItsHostRow()
    {
        var url = await ClaimOne("https://a.example/old", depth: 1);
        var update = new Redirected("/new").ToUpdate(url, TestData.Now);

        await Frontier().Complete(url, update, Fetch(), default);

        var target = await PostgresTestData.Row(
            postgres.DataSource, """SELECT * FROM "DiscoveredUrls" WHERE "NormalizedUrl" = 'https://a.example/new'""");
        ((int)target.Depth).Should().Be(1);
        ((string)target.Status).Should().Be("Idle");
        ((int)target.CrawlJobId).Should().Be(url.CrawlJobId);
    }

    [Fact]
    public async Task CompleteOfAnOffSiteRedirectDoesNotAddTheTarget()
    {
        var url = await ClaimOne("https://a.example/old");
        var update = new Redirected("https://elsewhere.example/").ToUpdate(url, TestData.Now);

        await Frontier().Complete(url, update, Fetch(), default);

        var count = await PostgresTestData.Scalar<long>(postgres.DataSource, """SELECT count(*) FROM "DiscoveredUrls" """);
        count.Should().Be(1);
    }

    [Fact]
    public async Task AddSeedsInsertsSeedsAndHostsAndIgnoresDuplicates()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, status: "Pending");
        var seeds = new[] { new NewUrl(job, "https://a.example/", "https://a.example/", "a.example", 0) };

        await Frontier().AddSeeds(job, seeds, default);
        await Frontier().AddSeeds(job, seeds, default);

        var urls = await PostgresTestData.Scalar<long>(postgres.DataSource, """SELECT count(*) FROM "DiscoveredUrls" """);
        var hosts = await PostgresTestData.Scalar<long>(postgres.DataSource, """SELECT count(*) FROM "Hosts" """);
        (urls, hosts).Should().Be((1L, 1L));
    }

    [Fact]
    public async Task AddSeedsStopsAtTheMaximumNumberOfUrlsOfTheJob()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, maxUrls: 2);
        var seeds = Enumerable.Range(0, 5)
            .Select(i => new NewUrl(job, $"https://a.example/{i}", $"https://a.example/{i}", "a.example", 0))
            .ToList();

        await Frontier().AddSeeds(job, seeds, default);

        var urls = await PostgresTestData.Rows<string>(
            postgres.DataSource, """SELECT "NormalizedUrl" FROM "DiscoveredUrls" ORDER BY "Id" """);
        urls.Should().Equal("https://a.example/0", "https://a.example/1");
    }
}
