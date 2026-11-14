using System.Text.Json;
using FluentAssertions;
using SamoBot.Adapters.Postgres;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresCrawlEventPublisherTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task PublishStoresTheEventAndNotifiesListenersWithTheFlatEvent()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await using var listener = await postgres.DataSource.OpenConnectionAsync();
        string? received = null;
        listener.Notification += (_, args) => received = args.Payload;
        await using (var listen = new Npgsql.NpgsqlCommand($"LISTEN {PostgresCrawlEventPublisher.Channel}", listener))
        {
            await listen.ExecuteNonQueryAsync();
        }

        await new PostgresCrawlEventPublisher(postgres.DataSource).Publish(job, "FetchCompleted", new { url = "https://a.example/" }, default);
        await listener.WaitAsync(TimeSpan.FromSeconds(5));

        var stored = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "CrawlJobEvents" """);
        ((string)stored.EventType).Should().Be("FetchCompleted");
        received.Should().NotBeNull();
        using var envelope = JsonDocument.Parse(received!);
        var root = envelope.RootElement;
        root.GetProperty("crawlJobId").GetInt32().Should().Be(job);
        root.GetProperty("id").GetInt64().Should().Be((long)stored.Id);
        root.GetProperty("eventType").GetString().Should().Be("FetchCompleted");
        root.GetProperty("url").GetString().Should().Be("https://a.example/");
        root.GetProperty("at").GetDateTimeOffset().Should().BeCloseTo(DateTimeOffset.UtcNow, TimeSpan.FromMinutes(1));
        root.TryGetProperty("payload", out _).Should().BeFalse();
        root.TryGetProperty("eventId", out _).Should().BeFalse();
    }

    [Fact]
    public async Task PublishStoresAnOversizedEventButSendsNoNotification()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await using var listener = await postgres.DataSource.OpenConnectionAsync();
        var received = new List<string>();
        listener.Notification += (_, args) => received.Add(args.Payload);
        await using (var listen = new Npgsql.NpgsqlCommand($"LISTEN {PostgresCrawlEventPublisher.Channel}", listener))
        {
            await listen.ExecuteNonQueryAsync();
        }

        await new PostgresCrawlEventPublisher(postgres.DataSource).Publish(job, "FetchStarted", new { url = new string('a', 9000) }, default);
        await listener.WaitAsync(TimeSpan.FromSeconds(1));

        received.Should().BeEmpty();
        (await PostgresTestData.Scalar<int>(postgres.DataSource, """SELECT count(*)::int FROM "CrawlJobEvents" """)).Should().Be(1);
    }
}
