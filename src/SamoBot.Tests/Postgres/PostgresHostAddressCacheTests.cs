using System.Net;
using FluentAssertions;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Ports;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

/// <summary>UNVERIFIED: written without Docker. Run with <c>--filter "Category=Integration"</c>.</summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresHostAddressCacheTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresHostAddressCache Cache => new(postgres.DataSource);

    private static ResolvedHost Resolved(string host, params string[] addresses) =>
        new(host, addresses.Select(IPAddress.Parse).ToList(), TestData.Now);

    [Fact]
    public async Task FindOfAnUnknownHostReturnsNull() =>
        (await Cache.Find("nobody.example", default)).Should().BeNull();

    [Fact]
    public async Task SaveThenFindRoundTripsTheAddressesAndTheTime()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");

        await Cache.Save(Resolved("a.example", "8.8.8.8", "2606:4700:4700::1111"), default);
        var found = await Cache.Find("a.example", default);

        found!.Addresses.Select(a => a.ToString()).Should().Equal("8.8.8.8", "2606:4700:4700::1111");
        found.ResolvedAt.Should().Be(TestData.Now);
    }

    [Fact]
    public async Task SaveAlsoCoversTheHostRowWithAPort()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example:8443/");

        await Cache.Save(Resolved("a.example", "8.8.8.8"), default);

        (await Cache.Find("a.example", default))!.Addresses.Should().ContainSingle();
    }

    [Fact]
    public async Task ClearForgetsTheAddresses()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");
        await Cache.Save(Resolved("a.example", "8.8.8.8"), default);

        await Cache.Clear("a.example", default);

        (await Cache.Find("a.example", default)).Should().BeNull();
    }

    [Fact]
    public async Task SaveForAHostWithoutARowStoresNothingAndDoesNotThrow()
    {
        await Cache.Save(Resolved("ghost.example", "8.8.8.8"), default);

        (await Cache.Find("ghost.example", default)).Should().BeNull();
    }
}
