using FluentAssertions;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;
using Microsoft.Extensions.Time.Testing;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresSmallAdapterTests(PostgresFixture postgres) : IAsyncLifetime
{
    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private async Task<int> ParsedDocument(string title)
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var url = await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://a.example/{title}");
        var fetch = await PostgresTestData.InsertFetch(postgres.DataSource, url, parsed: true);
        return await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """INSERT INTO "ParsedDocuments" ("UrlFetchId", "Title") VALUES (@fetch, @title) RETURNING "Id" """,
            new { fetch, title });
    }

    [Fact]
    public async Task IndexBacklogClaimsOnlyPagesThatAreNotIndexedAndMarksThemIndexed()
    {
        var first = await ParsedDocument("one");
        var second = await ParsedDocument("two");
        var queue = new PostgresIndexBacklog(postgres.DataSource, _time);

        var claimed = await queue.Claim(10, default);
        await queue.MarkIndexed([first], default);
        var rest = await queue.Claim(10, default);

        claimed.Select(p => p.PageId).Should().Equal(first, second);
        claimed[0].Title.Should().Be("one");
        claimed[0].Description.Should().BeEmpty();
        claimed[0].Host.Should().Be("a.example");
        claimed[0].Url.Should().Be("https://a.example/one");
        claimed[0].DiscoveredUrlId.Should().BePositive();
        claimed[0].FetchedAt.Should().NotBeNull();
        rest.Select(p => p.PageId).Should().Equal(second);
    }

    [Fact]
    public async Task JobPageReaderListsPagesCountsThemAndListsEventsAfterAnId()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/1");
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/2", status: "Disabled");
        foreach (var eventType in new[] { "One", "Two" })
        {
            await PostgresTestData.Scalar<int>(
                postgres.DataSource,
                """INSERT INTO "CrawlJobEvents" ("CrawlJobId", "EventType") VALUES (@job, @eventType) RETURNING "Id" """,
                new { job, eventType });
        }

        var reader = new PostgresJobPageReader(postgres.DataSource);

        var pages = await reader.ListPages(job, 10, 1, default);
        var total = await reader.CountPages(job, default);
        var events = await reader.ListEvents(job, 1, 10, default);

        total.Should().Be(2);
        pages.Should().ContainSingle().Which.Status.Should().Be(UrlStatus.Disabled);
        events.Should().ContainSingle().Which.EventType.Should().Be("Two");
    }

    [Fact]
    public async Task RobotsStoreSavesReplacesAndReadsRulesAndSetsTheHostDelay()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");
        var store = new PostgresRobotsStore(postgres.DataSource);

        await store.Save(new RobotsRules("a.example", "User-agent: *\nDisallow: /x\n", TestData.Now, TestData.Now.AddDays(1)), default);
        await store.Save(new RobotsRules("a.example", "User-agent: *\nCrawl-delay: 3\n", TestData.Now, TestData.Now.AddDays(2)), default);

        var rules = await store.Find("a.example", default);
        rules!.CrawlDelay.Should().Be(TimeSpan.FromSeconds(3));
        rules.ExpiresAt.Should().Be(TestData.Now.AddDays(2));
        (await store.Find("missing.example", default)).Should().BeNull();
        var delay = await PostgresTestData.Scalar<int>(postgres.DataSource, """SELECT "CrawlDelayMs" FROM "Hosts" WHERE "Host" = 'a.example'""");
        delay.Should().Be(3000);
    }
}
