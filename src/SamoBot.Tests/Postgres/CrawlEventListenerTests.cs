using System.Text.Json;
using FluentAssertions;
using Microsoft.Extensions.Logging.Abstractions;
using SamoBot.Adapters.Postgres;
using SamoBot.Api.Hosting;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class CrawlEventListenerTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task ListenerSendsAPublishedEventToTheGroupOfItsJob()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var hub = new RecordingHubContext();
        using var listener = new CrawlEventListener(postgres.DataSource, hub, TimeProvider.System, NullLogger<CrawlEventListener>.Instance);
        await listener.StartAsync(default);

        var publisher = new PostgresCrawlEventPublisher(postgres.DataSource);
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (hub.Sent.Count == 0 && DateTime.UtcNow < deadline)
        {
            await publisher.Publish(job, "JobStarted", new { }, default);
            await Task.Delay(200);
        }

        await listener.StopAsync(default);
        var sent = hub.Sent.Should().NotBeEmpty().And.Subject.First();
        sent.Group.Should().Be($"crawlJob:{job}");
        sent.Method.Should().Be("CrawlEvent");
        var json = JsonElement.Parse((string)sent.Args[0]!);
        json.GetProperty("eventType").GetString().Should().Be("JobStarted");
        json.GetProperty("crawlJobId").GetInt32().Should().Be(job);
        json.GetProperty("at").GetString().Should().NotBeNullOrEmpty();
        json.TryGetProperty("payload", out _).Should().BeFalse();
    }
}
