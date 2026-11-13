using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresParseBacklogTests(PostgresFixture postgres) : IAsyncLifetime
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresParseBacklog Queue() => new(postgres.DataSource, _time);

    private async Task<(int JobId, int UrlId)> JobWithUrl(string url = "https://a.example/", int? maxUrls = null)
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, maxUrls: maxUrls);
        var urlId = await PostgresTestData.InsertUrl(postgres.DataSource, job, url, status: "Idle");
        return (job, urlId);
    }

    [Fact]
    public async Task ClaimReturnsUnparsedFetchesWithTheScopeOfTheirJob()
    {
        var (job, urlId) = await JobWithUrl();
        var fetchId = await PostgresTestData.InsertFetch(postgres.DataSource, urlId, "obj-1");
        await PostgresTestData.InsertFetch(postgres.DataSource, urlId, "obj-2", parsed: true);

        var claimed = await Queue().Claim(10, Lease, default);

        var fetch = claimed.Should().ContainSingle().Subject;
        fetch.FetchId.Should().Be(fetchId);
        fetch.CrawlJobId.Should().Be(job);
        fetch.ObjectName.Should().Be("obj-1");
        fetch.AllowedHosts.Should().Equal("a.example");
        fetch.Url.Should().Be("https://a.example/");
    }

    [Fact]
    public async Task ClaimDoesNotReturnALeasedFetchUntilTheLeaseExpires()
    {
        var (_, urlId) = await JobWithUrl();
        var fetchId = await PostgresTestData.InsertFetch(postgres.DataSource, urlId);

        var first = await Queue().Claim(10, Lease, default);
        _time.Advance(TimeSpan.FromMinutes(1));
        var whileLeased = await Queue().Claim(10, Lease, default);
        _time.Advance(TimeSpan.FromMinutes(10));
        var afterExpiry = await Queue().Claim(10, Lease, default);

        first.Should().ContainSingle();
        whileLeased.Should().BeEmpty();
        afterExpiry.Should().ContainSingle().Which.FetchId.Should().Be(fetchId);
    }

    [Fact]
    public async Task ClaimParallelCallsReturnNoCommonFetch()
    {
        var (_, urlId) = await JobWithUrl();
        for (var i = 0; i < 10; i++)
        {
            await PostgresTestData.InsertFetch(postgres.DataSource, urlId, $"obj-{i}");
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 4).Select(_ => Queue().Claim(10, Lease, default)));

        var ids = results.SelectMany(r => r).Select(f => f.FetchId).ToList();
        ids.Should().OnlyHaveUniqueItems().And.HaveCount(10);
    }

    [Fact]
    public async Task CompleteStoresThePageAddsLinksAndMarksTheFetchParsed()
    {
        var (job, urlId) = await JobWithUrl();
        await PostgresTestData.InsertFetch(postgres.DataSource, urlId);
        var fetch = (await Queue().Claim(10, Lease, default)).Single();
        var page = new ParsedPage { Title = "Hello", BodyText = "Body", Links = [new ParsedLink { Url = "/x" }] };
        var links = new[] { new NewUrl(job, "https://a.example/x", "https://a.example/x", "a.example", 1) };

        await Queue().Complete(fetch, page, links, default);

        var doc = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "ParsedDocuments" WHERE "UrlFetchId" = @id""", new { id = fetch.FetchId });
        ((string)doc.Title).Should().Be("Hello");
        ((object?)doc.IndexedAt).Should().BeNull();
        var child = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "DiscoveredUrls" WHERE "NormalizedUrl" = 'https://a.example/x'""");
        ((int)child.Depth).Should().Be(1);
        var fetchRow = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "UrlFetches" WHERE "Id" = @id""", new { id = fetch.FetchId });
        ((DateTimeOffset)fetchRow.ParsedAt).Should().Be(TestData.Now);
        ((object?)fetchRow.LeaseUntil).Should().BeNull();
        (await Queue().Claim(10, Lease, default)).Should().BeEmpty();
    }

    [Fact]
    public async Task CompleteCalledTwiceKeepsOneDocumentAndTruncatesLongFields()
    {
        var (_, urlId) = await JobWithUrl();
        await PostgresTestData.InsertFetch(postgres.DataSource, urlId);
        var fetch = (await Queue().Claim(10, Lease, default)).Single();
        var page = new ParsedPage { Title = new string('t', 900), Language = "en-US-x-very-long" };

        await Queue().Complete(fetch, page, [], default);
        await Queue().Complete(fetch, page, [], default);

        var count = await PostgresTestData.Scalar<long>(postgres.DataSource, """SELECT count(*) FROM "ParsedDocuments" """);
        var title = await PostgresTestData.Scalar<string>(postgres.DataSource, """SELECT "Title" FROM "ParsedDocuments" """);
        count.Should().Be(1);
        title.Should().HaveLength(500);
    }
}
