using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Core.Sitemaps;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

/// <summary>UNVERIFIED in the authoring environment: written without Docker, so these tests have never run.</summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresSitemapQueueTests(PostgresFixture postgres) : IAsyncLifetime
{
    private const string Host = "a.example";

    private static readonly TimeSpan Lease = SitemapBatchHandler.Lease;

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public async Task InitializeAsync()
    {
        await postgres.Reset();
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """WITH a AS (DELETE FROM "SitemapUrls"), b AS (DELETE FROM "SitemapFiles"), c AS (DELETE FROM "SitemapRequests") SELECT 1""");
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresSitemapStore Store => new(postgres.DataSource, _time);

    private static SitemapFileContent File(string host, string loc) =>
        new(new SitemapFile(host, $"https://{host}/s.xml", null, SitemapKind.UrlSet, 1, TestData.Now), [new SitemapEntry(loc, loc, null)]);

    private static SitemapWalkResult Found(string host = Host, string loc = "https://a.example/p") => new([File(host, loc)], false);

    private static SitemapWalkResult Nothing(bool gone = false) => new([], gone);

    private async Task<SitemapClaim> Claim(string host = Host)
    {
        await Store.Enqueue(host, default);
        return (await Store.ClaimNext(Lease, default))!;
    }

    private Task<int> Count(string table) => PostgresTestData.Scalar<int>(postgres.DataSource, $"""SELECT count(*)::int FROM "{table}" """);

    private Task<int> Attempts() =>
        PostgresTestData.Scalar<int>(postgres.DataSource, """SELECT "Attempts" FROM "SitemapRequests" WHERE "Host" = 'a.example'""");

    [Fact]
    public async Task ARequestMadeDuringARefreshIsStillQueuedAfterComplete()
    {
        var claim = await Claim();
        _time.Advance(TimeSpan.FromSeconds(1));
        await Store.Enqueue(Host, default);

        (await Store.Complete(claim, Found(), default)).Should().BeTrue();

        var completedAt = await PostgresTestData.Scalar<DateTimeOffset?>(
            postgres.DataSource, """SELECT "CompletedAt" FROM "SitemapRequests" WHERE "Host" = 'a.example'""");
        completedAt.Should().BeNull();
        (await Store.ClaimNext(Lease, default))!.Host.Should().Be(Host);
    }

    [Fact]
    public async Task CompleteOfATakenOverLeaseWritesNothingAndTheNewOwnerCompletes()
    {
        var first = await Claim();
        _time.Advance(Lease + TimeSpan.FromSeconds(1));
        var second = (await Store.ClaimNext(Lease, default))!;

        (await Store.Complete(first, Found(), default)).Should().BeFalse();
        (await Count("SitemapFiles")).Should().Be(0);
        (await Store.Complete(second, Found(), default)).Should().BeTrue();
        (await Count("SitemapFiles")).Should().Be(1);
        (await Count("SitemapUrls")).Should().Be(1);
    }

    [Fact]
    public async Task ReserveFetchSlotGivesTwoParallelCallersDifferentSlots()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/x");
        await PostgresTestData.Scalar<int>(
            postgres.DataSource, """UPDATE "Hosts" SET "CrawlDelayMs" = 10000, "NextAllowedAt" = @now WHERE "Host" = 'a.example' RETURNING 1""", new { now = TestData.Now });

        var waits = await Task.WhenAll(Store.ReserveFetchSlot(Host, default), Store.ReserveFetchSlot(Host, default));

        var sorted = waits.Select(w => w.TotalSeconds).Order().ToArray();
        sorted[0].Should().BeApproximately(0, 0.01);
        sorted[1].Should().BeApproximately(10, 0.01);
    }

    [Fact]
    public async Task ClaimCountsAttemptsAndStopsAfterTheMaximum()
    {
        await Store.Enqueue(Host, default);
        for (var attempt = 1; attempt <= SitemapBatchHandler.MaxAttempts; attempt++)
        {
            (await Store.ClaimNext(Lease, default)).Should().NotBeNull();
            (await Attempts()).Should().Be(attempt);
            _time.Advance(Lease + TimeSpan.FromSeconds(1));
        }

        (await Store.ClaimNext(Lease, default)).Should().BeNull();
    }

    [Fact]
    public async Task AHostThatFailedTheMaximumIsQueuedAgainAfterTheRefreshInterval()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, "Completed");
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/x");
        await Store.Enqueue(Host, default);
        for (var attempt = 0; attempt < SitemapBatchHandler.MaxAttempts; attempt++)
        {
            await Store.ClaimNext(Lease, default);
            _time.Advance(Lease + TimeSpan.FromSeconds(1));
        }

        await Store.EnqueueFinishedHosts(SitemapBatchHandler.RefreshAfter, default);
        (await Store.ClaimNext(Lease, default)).Should().BeNull();
        _time.Advance(SitemapBatchHandler.RefreshAfter + TimeSpan.FromDays(1));
        await Store.EnqueueFinishedHosts(SitemapBatchHandler.RefreshAfter, default);

        (await Store.ClaimNext(Lease, default))!.Host.Should().Be(Host);
        (await Attempts()).Should().Be(1);
    }

    [Fact]
    public async Task CompleteOfAGoneWalkDeletesTheOldFilesButATransientFailureKeepsThem()
    {
        await Store.Complete(await Claim(), Found(), default);
        (await Count("SitemapUrls")).Should().Be(1);

        await Store.Complete(await Claim(), Nothing(), default);
        (await Count("SitemapUrls")).Should().Be(1);

        await Store.Complete(await Claim(), Nothing(gone: true), default);
        (await Count("SitemapFiles")).Should().Be(0);
        (await Count("SitemapUrls")).Should().Be(0);
    }

    [Fact]
    public async Task CompleteStoresALocOfTheMaximumLengthOnAHostOfTheMaximumLength()
    {
        var host = string.Join('.', new string('a', 63), new string('b', 63), new string('c', 63), new string('d', 61));
        var prefix = $"https://{host}/";
        var loc = prefix + new string('x', SitemapLimits.MaxLocBytes - prefix.Length);
        host.Length.Should().Be(253);
        loc.Length.Should().Be(SitemapLimits.MaxLocBytes);

        (await Store.Complete(await Claim(host), Found(host, loc), default)).Should().BeTrue();

        (await Count("SitemapUrls")).Should().Be(1);
    }
}
