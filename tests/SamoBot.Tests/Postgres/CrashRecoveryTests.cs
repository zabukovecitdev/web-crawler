using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresCollection.Name)]
[Trait("Category", "Integration")]
public class CrashRecoveryTests(PostgresFixture postgres) : IAsyncLifetime
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    [Fact]
    public async Task ABatchThatIsNeverCompleted_IsClaimedAgainAfterTheLeaseExpires()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, allowedHosts: "[]");
        for (var i = 0; i < 8; i++)
        {
            await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://h{i}.example/");
        }

        var crashed = await new PostgresUrlFrontier(postgres.DataSource, _time).Claim(20, Lease, default);
        // The worker dies here. Nothing completes the batch.
        _time.Advance(Lease + TimeSpan.FromSeconds(1));
        var recovered = await new PostgresUrlFrontier(postgres.DataSource, _time).Claim(20, Lease, default);

        crashed.Should().HaveCount(8);
        recovered.Select(url => url.Id).Should().BeEquivalentTo(crashed.Select(url => url.Id));
    }

    [Fact]
    public async Task AFetchThatIsNeverParsed_IsClaimedAgainAfterTheLeaseExpires()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var url = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");
        await PostgresTestData.InsertFetch(postgres.DataSource, url);

        var crashed = await new PostgresParseQueue(postgres.DataSource, _time).Claim(10, Lease, default);
        _time.Advance(Lease + TimeSpan.FromSeconds(1));
        var recovered = await new PostgresParseQueue(postgres.DataSource, _time).Claim(10, Lease, default);

        recovered.Select(fetch => fetch.FetchId).Should().Equal(crashed.Select(fetch => fetch.FetchId));
    }
}
