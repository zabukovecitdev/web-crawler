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
    private static readonly TimeSpan Hold = TimeSpan.FromSeconds(30);

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

        var results = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Frontier().Claim(12, Lease, Hold, true, default)));

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

        var claimed = await Frontier().Claim(10, Lease, Hold, true, default);
        await Frontier().Complete(claimed[0], new Succeeded(TimeSpan.FromDays(1)).ToUpdate(claimed[0], TestData.Now), Fetch(), default);
        var immediately = await Frontier().Claim(10, Lease, Hold, true, default);
        _time.Advance(TimeSpan.FromSeconds(2));
        var afterDelay = await Frontier().Claim(10, Lease, Hold, true, default);

        claimed.Select(u => u.Id).Should().Equal(first);
        immediately.Should().BeEmpty();
        afterDelay.Should().ContainSingle().Which.Url.Should().Be("https://a.example/mid");
    }

    [Fact]
    public async Task ClaimDoesNotReturnUrlsOfAPausedJob()
    {
        var paused = await PostgresTestData.InsertJob(postgres.DataSource, status: "Paused");
        await PostgresTestData.InsertUrl(postgres.DataSource, paused, "https://a.example/");

        (await Frontier().Claim(10, Lease, Hold, true, default)).Should().BeEmpty();
    }

    [Fact]
    public async Task ClaimReturnsTheUrlAgainAfterTheLeaseExpires()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var id = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");

        var first = await Frontier().Claim(10, Lease, Hold, true, default);
        _time.Advance(TimeSpan.FromSeconds(30));
        var whileLeased = await Frontier().Claim(10, Lease, Hold, true, default);
        _time.Advance(TimeSpan.FromMinutes(2));
        var afterExpiry = await Frontier().Claim(10, Lease, Hold, true, default);

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

        var claimed = await Frontier().Claim(5, Lease, Hold, true, default);

        claimed.Should().ContainSingle().Which.Id.Should().Be(due);
    }

    [Fact]
    public async Task ClaimReturnsTheCrawlDelayOfTheHost()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");
        await PostgresTestData.Scalar<int>(
            postgres.DataSource, """UPDATE "Hosts" SET "CrawlDelayMs" = 2500 WHERE "Host" = 'a.example' RETURNING 1""");

        var claimed = await Frontier().Claim(1, Lease, Hold, true, default);

        claimed.Should().ContainSingle().Which.CrawlDelayMs.Should().Be(2500);
    }

    private async Task<ClaimedUrl> ClaimOne(string url, int depth = 0)
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, url, depth);
        return (await Frontier().Claim(1, Lease, Hold, true, default)).Single();
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
    public async Task TheKeptResponseHeadersOfACompletedFetchReachTheParser()
    {
        var url = await ClaimOne("https://a.example/");
        var update = new Succeeded(TimeSpan.FromDays(1)).ToUpdate(url, TestData.Now);
        var headers = new ResponseHeaders(
        [
            new KeyValuePair<string, IReadOnlyList<string>>("X-Robots-Tag", ["noindex", "samobot: nofollow"]),
            new KeyValuePair<string, IReadOnlyList<string>>("Link", ["</main>; rel=canonical"])
        ]);

        await Frontier().Complete(url, update, Fetch() with { ResponseHeaders = headers }, default);

        var claimed = await new PostgresParseBacklog(postgres.DataSource, _time).Claim(10, TimeSpan.FromMinutes(5), default);
        var kept = claimed.Should().ContainSingle().Subject.ResponseHeaders;
        kept.Values("x-robots-tag").Should().Equal("noindex", "samobot: nofollow");
        kept.Values("link").Should().Equal("</main>; rel=canonical");
    }

    [Fact]
    public async Task AFetchWithoutKeptHeadersStoresNullAndReadsAsNone()
    {
        var url = await ClaimOne("https://a.example/");

        await Frontier().Complete(url, new Succeeded(TimeSpan.FromDays(1)).ToUpdate(url, TestData.Now), Fetch(), default);

        (await PostgresTestData.Scalar<string?>(postgres.DataSource, """SELECT "ResponseHeaders" FROM "UrlFetches" """)).Should().BeNull();
        var claimed = await new PostgresParseBacklog(postgres.DataSource, _time).Claim(10, TimeSpan.FromMinutes(5), default);
        claimed.Single().ResponseHeaders.IsEmpty.Should().BeTrue();
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
    public async Task CompleteOfABlockedUrlDisablesItRecordsStatusZeroAndNeverClaimsItAgain()
    {
        var url = await ClaimOne("https://a.example/");
        var fetch = new FetchRecord { FetchedAt = TestData.Now, StatusCode = 0, ResponseTimeMs = 0, FinalUrl = url.Url };

        await Frontier().Complete(url, new Blocked().ToUpdate(url, TestData.Now), fetch, default);
        _time.Advance(TimeSpan.FromDays(30));

        var row = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "DiscoveredUrls" WHERE "Id" = @id""", new { id = url.Id });
        ((string)row.Status).Should().Be("Disabled");
        ((string)row.FailReason).Should().Be("blocked-address");
        ((object?)row.NextCrawlAt).Should().BeNull();
        var fetchStatus = await PostgresTestData.Scalar<int>(
            postgres.DataSource, """SELECT "StatusCode" FROM "UrlFetches" WHERE "DiscoveredUrlId" = @id""", new { id = url.Id });
        fetchStatus.Should().Be(0);
        (await Frontier().Claim(10, Lease, Hold, true, default)).Should().BeEmpty();
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
    public async Task AddSeedsLeavesOutThePathsThatTheRulesOfTheJobExclude()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.Execute(
            postgres.DataSource,
            """UPDATE "CrawlJobs" SET "PathRules" = '[{"kind":"Exclude","prefix":"/blog/"},{"kind":"Include","prefix":"/blog/2026/"},{"kind":"Exclude","prefix":"/x","exact":true}]' WHERE "Id" = @job""",
            new { job });
        var paths = new[] { "/", "/blog/2016/a", "/blog/2026/b", "/blog", "/x", "/x/y", "/q?blog=1" };
        var seeds = paths.Select(path => new NewUrl(job, $"https://a.example{path}", $"https://a.example{path}", "a.example", 0)).ToList();

        await Frontier().AddSeeds(job, seeds, default);

        var stored = await PostgresTestData.Rows<string>(postgres.DataSource, """SELECT "Url" FROM "DiscoveredUrls" WHERE "CrawlJobId" = @job ORDER BY "Url" """, new { job });
        stored.Should().Equal("https://a.example/", "https://a.example/blog/2026/b", "https://a.example/q?blog=1", "https://a.example/x/y");
    }

    [Fact]
    public async Task ClaimLeavesTheUrlsThatNeedTheBrowserWhileItIsUnavailable()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var plain = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/plain");
        var browser = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://b.example/js");
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "DiscoveredUrls" SET "UseJsRendering" = true WHERE "Id" = @browser""", new { browser });

        var without = await Frontier().Claim(10, Lease, Hold, false, default);
        _time.Advance(TimeSpan.FromMinutes(2));
        var with = await Frontier().Claim(10, Lease, Hold, true, default);

        without.Select(url => url.Id).Should().Equal(plain);
        with.Select(url => url.Id).Should().Contain(browser);
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

    [Fact]
    public async Task RequeueRefetchesSkippedStoredTypesAndReparsesStoredPagesSoTheJobIsNotDone()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var markdown = await UrlWithLastFetch(job, "https://a.example/docs", "text/markdown; charset=utf-8", objectName: null, parsed: false);
        var html = await UrlWithLastFetch(job, "https://a.example/", "text/html", objectName: "obj", parsed: true);
        var pdf = await UrlWithLastFetch(job, "https://a.example/a.pdf", "application/pdf", objectName: null, parsed: false);
        _time.Advance(TimeSpan.FromHours(1));

        var work = await Frontier().RequeueForReprocess(job, StoredMediaTypes.All, default);

        work.Should().Be(new RequeuedWork(1, 1));
        var refetched = await UrlState(markdown);
        ((DateTimeOffset)refetched.NextCrawlAt).Should().Be(_time.GetUtcNow());
        ((object?)refetched.LastCrawlAt).Should().BeNull();
        ((object?)(await UrlState(pdf)).LastCrawlAt).Should().NotBeNull();
        (await PostgresTestData.Scalar<int>(postgres.DataSource,
            """SELECT count(*) FROM "UrlFetches" WHERE "DiscoveredUrlId" = @html AND "ParsedAt" IS NULL""", new { html })).Should().Be(1);
        (await new PostgresCrawlJobRepository(postgres.DataSource).CompleteFinishedJobs(_time.GetUtcNow(), default)).Should().BeEmpty();
    }

    private Task<dynamic> UrlState(int urlId) =>
        PostgresTestData.Row(postgres.DataSource, """SELECT "NextCrawlAt", "LastCrawlAt" FROM "DiscoveredUrls" WHERE "Id" = @urlId""", new { urlId });

    private async Task<int> UrlWithLastFetch(int job, string url, string contentType, string? objectName, bool parsed)
    {
        var urlId = await PostgresTestData.InsertUrl(postgres.DataSource, job, url, nextCrawlAt: TestData.Now.AddDays(1));
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            WITH f AS (
                INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ContentLength", "ObjectName", "ParsedAt")
                VALUES (@urlId, @now, 200, @contentType, 10, @objectName, CASE WHEN @parsed THEN @now END) RETURNING "Id")
            UPDATE "DiscoveredUrls" SET "LastFetchId" = (SELECT "Id" FROM f), "LastCrawlAt" = @now WHERE "Id" = @urlId RETURNING "Id"
            """,
            new { urlId, now = TestData.Now, contentType, objectName, parsed });
        return urlId;
    }
}
