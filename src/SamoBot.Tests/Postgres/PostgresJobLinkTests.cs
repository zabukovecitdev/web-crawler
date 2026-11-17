using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Api.Contracts;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

/// <summary>The link read model of a job and the requeue of the frontier. Run with <c>--filter "Category=Integration"</c>.</summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresJobLinkTests(PostgresFixture postgres) : IAsyncLifetime
{
    private const string NoIndex = """{"index":false,"directives":["noindex (meta robots)"]}""";

    private static readonly JsonSerializerOptions Web = new(JsonSerializerDefaults.Web);

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresJobLinkReader Reader => new(postgres.DataSource, _time);

    private PostgresUrlFrontier Frontier => new(postgres.DataSource, _time);

    private Task<int> Url(int job, string path, string status = "Idle") =>
        PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://a.example/{path}", status: status);

    private Task<int> Set(int urlId, string failReason, int failCount = 0) =>
        PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "DiscoveredUrls" SET "FailReason" = @failReason, "FailCount" = @failCount WHERE "Id" = @urlId RETURNING "Id" """,
            new { urlId, failReason, failCount });

    private async Task<int> Fetch(int urlId, int status = 200, string? objectName = "obj", string contentType = "text/html")
    {
        var fetchId = await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ObjectName")
            VALUES (@urlId, @at, @status, @contentType, @objectName) RETURNING "Id"
            """,
            new { urlId, at = TestData.Now, status, contentType, objectName });
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetchId, "LastCrawlAt" = @at WHERE "Id" = @urlId RETURNING "Id" """,
            new { fetchId, urlId, at = TestData.Now });
        return fetchId;
    }

    private Task<int> Parsed(int fetchId, bool indexed, string? policy = null) =>
        PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "ParsedDocuments" ("UrlFetchId", "Title", "ParsedAt", "IndexedAt", "Policy")
            VALUES (@fetchId, 'Title', @at, @indexedAt, @policy) RETURNING "Id"
            """,
            new { fetchId, at = TestData.Now, indexedAt = indexed ? TestData.Now : (DateTimeOffset?)null, policy });

    /// <summary>One link in every state, in the order of <see cref="LinkState"/>.</summary>
    private async Task<int> JobWithEveryState()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await Url(job, "queued");
        await Url(job, "fetching", "InFlight");
        await Set(await Url(job, "retrying"), "timeout", failCount: 2);
        await Fetch(await Url(job, "parsing"));
        await Parsed(await Fetch(await Url(job, "indexing")), indexed: false);
        await Parsed(await Fetch(await Url(job, "indexed")), indexed: true);
        await Parsed(await Fetch(await Url(job, "noindex")), indexed: true, NoIndex);
        await Fetch(await Url(job, "file.pdf"), objectName: null, contentType: "application/pdf");
        var missing = await Url(job, "missing", "Disabled");
        await Fetch(missing, 404, objectName: null);
        await Set(missing, "http-404");
        return job;
    }

    [Fact]
    public async Task ListDerivesTheStateOfEveryLink()
    {
        var job = await JobWithEveryState();

        var page = await Reader.List(job, new JobLinkQuery(JobLinkFilter.All, null, 50, 0), default);

        page.Total.Should().Be(9);
        page.Items.Select(link => link.State).Should().Equal(Enum.GetValues<LinkState>());
        page.Items.Single(link => link.State == LinkState.NotIndexed).Policy!.Directives.Should().Equal("noindex (meta robots)");
    }

    [Theory]
    [InlineData(JobLinkFilter.Problems, 2)]
    [InlineData(JobLinkFilter.NotIndexed, 8)]
    [InlineData(JobLinkFilter.Pending, 5)]
    [InlineData(JobLinkFilter.Indexed, 1)]
    public async Task ListFiltersByState(JobLinkFilter filter, int expected)
    {
        var job = await JobWithEveryState();

        var page = await Reader.List(job, new JobLinkQuery(filter, null, 50, 0), default);

        page.Total.Should().Be(expected);
        page.Items.Should().HaveCount(expected);
    }

    [Fact]
    public async Task ListSearchesTheUrlAndPages()
    {
        var job = await JobWithEveryState();

        var page = await Reader.List(job, new JobLinkQuery(JobLinkFilter.All, "IND", 2, 1), default);

        page.Total.Should().Be(3);
        page.Items.Select(link => link.Url).Should().Equal("https://a.example/indexed", "https://a.example/noindex");
    }

    [Fact]
    public async Task SummarizeCountsStatesAndGroupsTheProblemsOfEachJob()
    {
        var job = await JobWithEveryState();
        var other = await PostgresTestData.InsertJob(postgres.DataSource);
        await Url(other, "other");

        var summaries = await Reader.Summarize([job, other, 999], default);

        summaries.Keys.Should().BeEquivalentTo([job, other]);
        var summary = summaries[job];
        (summary.Total, summary.Indexed, summary.NotIndexed, summary.Skipped, summary.Failed, summary.Retrying).Should().Be((9, 1, 1, 1, 1, 1));
        summary.Problems.Should().BeEquivalentTo([new LinkProblem("http-404", "HTTP 404", 1), new LinkProblem("timeout", "Timed out", 1)]);
        summaries[other].Queued.Should().Be(1);
    }

    [Fact]
    public async Task RequeueOfTheFailedLinksLeavesTheOthersAlone()
    {
        var job = await JobWithEveryState();

        var count = await Frontier.Requeue(job, RequeueScope.Failed, default);

        count.Should().Be(2);
        var summary = (await Reader.Summarize([job], default))[job];
        (summary.Queued, summary.Failed, summary.Retrying, summary.Indexed).Should().Be((3, 0, 0, 1));
    }

    [Fact]
    public async Task RequeueOfEveryLinkSkipsTheOneInFlight()
    {
        var job = await JobWithEveryState();

        var count = await Frontier.Requeue(job, RequeueScope.All, default);

        count.Should().Be(8);
        var summary = (await Reader.Summarize([job], default))[job];
        (summary.Queued, summary.Fetching).Should().Be((8, 1));
    }

    [Fact]
    public async Task RequeueOfOneLinkMakesItDueAndKeepsItsFetchHistory()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var url = await Url(job, "page", "Disabled");
        await Fetch(url, 404, objectName: null);
        await Set(url, "http-404", failCount: 3);

        (await Frontier.Requeue(job, RequeueScope.Link(url), default)).Should().Be(1);
        (await Frontier.Requeue(job + 1, RequeueScope.Link(url), default)).Should().Be(0);

        var claimed = await Frontier.Claim(10, TimeSpan.FromMinutes(1), TimeSpan.FromSeconds(30), default);
        claimed.Should().ContainSingle().Which.FailCount.Should().Be(0);
        (await PostgresTestData.Scalar<long>(postgres.DataSource, """SELECT count(*) FROM "UrlFetches" """)).Should().Be(1);
    }

    [Fact]
    public async Task ARequeuedLinkKeepsARunningJobOpen()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await Parsed(await Fetch(await Url(job, "done")), indexed: true);
        var jobs = new PostgresCrawlJobRepository(postgres.DataSource);

        await Frontier.Requeue(job, RequeueScope.All, default);

        (await jobs.CompleteFinishedJobs(TestData.Now, default)).Should().BeEmpty();
    }

    [Fact]
    public async Task ARequeuedLinkKeepsItsLastFetchForThePageAndSiteScreens()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, "Running");
        var url = await Url(job, "page");
        await Parsed(await Fetch(url), indexed: true);
        var pages = new PostgresPageReader(postgres.DataSource);
        var sites = new PostgresSiteReader(postgres.DataSource, SiteOverviewCache.Disabled);

        await Frontier.Requeue(job, RequeueScope.All, default);

        var page = await pages.FindPage(url, default);
        page!.StatusCode.Should().Be(200);
        page.Parsed!.Title.Should().Be("Title");
        var site = (await sites.ListSites(new SiteQuery(null, SiteSort.Host, 50, 0), default)).Items.Single();
        site.Pages.Should().Be(1);
    }

    [Fact]
    public async Task ARequeuedLinkIsQueuedAndCountedAsWaitingUntilItIsFetchedAgain()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, "Running");
        var url = await Url(job, "page");
        await Parsed(await Fetch(url), indexed: true);
        var progress = new PostgresJobProgressReader(postgres.DataSource);
        var jobs = new PostgresCrawlJobRepository(postgres.DataSource);

        await Frontier.Requeue(job, RequeueScope.All, default);

        (await Reader.List(job, new JobLinkQuery(JobLinkFilter.All, null, 50, 0), default)).Items.Single().State.Should().Be(LinkState.Queued);
        (await progress.Read(job, default))!.Queued.Should().Be(1);
        (await jobs.CompleteFinishedJobs(TestData.Now, default)).Should().BeEmpty();

        var newFetch = await Fetch(url);

        (await PostgresTestData.Scalar<int>(postgres.DataSource, """SELECT "LastFetchId" FROM "DiscoveredUrls" WHERE "Id" = @url""", new { url })).Should().Be(newFetch);
        (await Reader.List(job, new JobLinkQuery(JobLinkFilter.All, null, 50, 0), default)).Items.Single().State.Should().Be(LinkState.Parsing);
        (await progress.Read(job, default))!.Queued.Should().Be(0);
    }

    [Fact]
    public async Task TheSummaryOfAJobKeepsItsJsonShapeForTheDashboard()
    {
        var jobId = await JobWithEveryState();
        var summary = (await Reader.Summarize([jobId], default))[jobId];
        var job = CrawlJob.Create(new NewCrawlJob("u1", ["https://a.example/"], 2, 10, false), TestData.Now).Value;

        var links = JsonSerializer.SerializeToElement(CrawlJobDto.From(job, summary), Web).GetProperty("links");

        links.EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo(
            "total", "queued", "fetching", "retrying", "parsing", "indexing", "indexed", "notIndexed", "skipped", "failed", "problems");
        links.EnumerateObject().Where(p => p.Name != "problems").Select(p => (p.Name, p.Value.GetInt32())).Should().BeEquivalentTo(
            [("total", 9), ("queued", 1), ("fetching", 1), ("retrying", 1), ("parsing", 1), ("indexing", 1), ("indexed", 1), ("notIndexed", 1), ("skipped", 1), ("failed", 1)]);
        links.GetProperty("problems").EnumerateArray()
            .Select(p => (p.GetProperty("reason").GetString(), p.GetProperty("label").GetString(), p.GetProperty("count").GetInt32()))
            .Should().BeEquivalentTo([("http-404", "HTTP 404", 1), ("timeout", "Timed out", 1)]);
        links.GetProperty("problems").EnumerateArray().First().EnumerateObject().Select(p => p.Name).Should().BeEquivalentTo("reason", "label", "count");
    }
}
