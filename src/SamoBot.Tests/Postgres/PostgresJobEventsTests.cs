using FluentAssertions;
using SamoBot.Adapters.Postgres;

namespace SamoBot.Tests.Postgres;

/// <summary>UNVERIFIED: written without Docker. Run with <c>--filter "Category=Integration"</c>.</summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresJobEventsTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresJobPageReader Reader => new(postgres.DataSource);

    private Task<long> AddEvent(int job, string eventType) =>
        PostgresTestData.Scalar<long>(
            postgres.DataSource,
            """INSERT INTO "CrawlJobEvents" ("CrawlJobId", "EventType") VALUES (@job, @eventType) RETURNING "Id" """,
            new { job, eventType });

    [Fact]
    public async Task RecentEventsAreTheNewestOnesInAscendingOrder()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var ids = new List<long>();
        for (var i = 1; i <= 5; i++)
        {
            ids.Add(await AddEvent(job, $"E{i}"));
        }

        var events = await Reader.ListRecentEvents(job, 2, default);

        events.Select(e => e.Id).Should().Equal(ids[3], ids[4]);
        events.Select(e => e.EventType).Should().Equal("E4", "E5");
    }

    [Fact]
    public async Task RecentEventsOfAnotherJobAreNotReturned()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var other = await PostgresTestData.InsertJob(postgres.DataSource);
        var mine = await AddEvent(job, "Mine");
        await AddEvent(other, "Theirs");

        var events = await Reader.ListRecentEvents(job, 10, default);

        events.Select(e => e.Id).Should().Equal(mine);
    }
}
