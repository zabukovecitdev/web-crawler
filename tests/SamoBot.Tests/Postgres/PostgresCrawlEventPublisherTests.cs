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
    public async Task PublishStoresTheEventAndNotifiesListenersWithTheEnvelope()
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
        envelope.RootElement.GetProperty("crawlJobId").GetInt32().Should().Be(job);
        envelope.RootElement.GetProperty("eventId").GetInt64().Should().Be((long)stored.Id);
        envelope.RootElement.GetProperty("eventType").GetString().Should().Be("FetchCompleted");
        envelope.RootElement.GetProperty("payload").GetProperty("url").GetString().Should().Be("https://a.example/");
    }
}
