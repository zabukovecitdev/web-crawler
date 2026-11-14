using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

/// <summary>
/// One request in flight per host. UNVERIFIED: written without Docker. Run with <c>--filter "Category=Integration"</c>.
/// The base delay of the hosts is 1 s, the hold 30 s, the URL lease 2 minutes.
/// </summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresHostHoldTests(PostgresFixture postgres) : IAsyncLifetime
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(2);
    private static readonly TimeSpan Hold = TimeSpan.FromSeconds(30);

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresUrlFrontier Frontier() => new(postgres.DataSource, _time);

    private FetchRecorder Recorder() =>
        new(new FakeContentStore(), Frontier(), new PolitenessPolicy(TimeSpan.FromSeconds(60)), _time);

    private Task<IReadOnlyList<ClaimedUrl>> Claim(int batch = 10) => Frontier().Claim(batch, Lease, Hold, default);

    private async Task<int> TwoUrlsOnOneHost()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/1", depth: 0);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/2", depth: 1);
        return job;
    }

    private Task<RecordedFetch> Finish(ClaimedUrl url, FetchResponse response) =>
        Recorder().Record(url, response, OutcomeClassifier.Classify(response, url.FailCount), default);

    private static FetchResponse Fast() => TestData.Response(200) with { ResponseTimeMs = 100 };

    [Fact]
    public async Task ASlowFetchKeepsTheHostClosedPastTheBaseDelayUntilItIsCompleted()
    {
        await TwoUrlsOnOneHost();

        var first = await Claim();
        var sameInstant = await Claim();
        _time.Advance(TimeSpan.FromSeconds(10));
        var afterBaseDelayBeforeComplete = await Claim();

        first.Should().ContainSingle().Which.Url.Should().Be("https://a.example/1");
        sameInstant.Should().BeEmpty();
        afterBaseDelayBeforeComplete.Should().BeEmpty();
    }

    [Fact]
    public async Task CompletingAFastFetchOpensTheHostAfterTheDelayOnly()
    {
        await TwoUrlsOnOneHost();
        var first = (await Claim()).Single();

        await Finish(first, Fast());
        var immediately = await Claim();
        _time.Advance(TimeSpan.FromMilliseconds(999));
        var justBefore = await Claim();
        _time.Advance(TimeSpan.FromMilliseconds(1));
        var afterDelay = await Claim();

        immediately.Should().BeEmpty();
        justBefore.Should().BeEmpty();
        afterDelay.Should().ContainSingle().Which.Url.Should().Be("https://a.example/2");
    }

    [Fact]
    public async Task TheDelayIsBookedFromTheCompletionNotFromTheClaim()
    {
        await TwoUrlsOnOneHost();
        var first = (await Claim()).Single();
        _time.Advance(TimeSpan.FromSeconds(8));

        await Finish(first, Fast());
        var atCompletion = await Claim();
        _time.Advance(TimeSpan.FromSeconds(1));
        var afterDelay = await Claim();

        atCompletion.Should().BeEmpty();
        afterDelay.Should().ContainSingle();
    }

    [Fact]
    public async Task ARetryAfterOfThirtySecondsIsRespectedAfterTheCompletion()
    {
        await TwoUrlsOnOneHost();
        var first = (await Claim()).Single();

        await Finish(first, TestData.Response(429) with { RetryAfter = TimeSpan.FromSeconds(30), ResponseTimeMs = 50 });
        _time.Advance(TimeSpan.FromSeconds(29));
        var before = await Claim();
        _time.Advance(TimeSpan.FromSeconds(1));
        var after = await Claim();

        before.Should().BeEmpty();
        after.Should().ContainSingle().Which.Url.Should().Be("https://a.example/2");
    }

    [Fact]
    public async Task ABlockedAddressAndARobotsDisallowReleaseTheHoldWithTheBaseDelay()
    {
        var job = await TwoUrlsOnOneHost();
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://b.example/1");
        var claimed = await Claim();
        var a = claimed.Single(u => u.Host == "a.example");
        var b = claimed.Single(u => u.Host == "b.example");

        await Finish(a, FetchResponse.BlockedAddress(5));
        await Recorder().RecordDisallowed(b, default);
        _time.Advance(TimeSpan.FromSeconds(1));
        var next = await Claim();

        next.Should().ContainSingle().Which.Url.Should().Be("https://a.example/2");
    }

    [Fact]
    public async Task AnAbandonedClaimOpensTheHostAgainWhenTheHoldEnds()
    {
        await TwoUrlsOnOneHost();
        await Claim();
        // The worker dies here. Nothing completes the URL.

        _time.Advance(Hold - TimeSpan.FromSeconds(1));
        var insideHold = await Claim();
        _time.Advance(TimeSpan.FromSeconds(2));
        var afterHold = await Claim();

        insideHold.Should().BeEmpty();
        afterHold.Should().ContainSingle().Which.Url.Should().Be("https://a.example/2");
    }

    [Fact]
    public async Task TheLeaseRecoversTheAbandonedUrlItself()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/1");
        var abandoned = await Claim();

        _time.Advance(Lease + TimeSpan.FromSeconds(1));
        var recovered = await Claim();

        recovered.Select(u => u.Id).Should().Equal(abandoned.Select(u => u.Id));
    }

    [Fact]
    public async Task DifferentHostsAreClaimedTogetherAndOneHoldDoesNotBlockTheOther()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, allowedHosts: "[]");
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/1");
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/2");
        var first = await Claim();
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://b.example/1");
        var second = await Claim();

        first.Should().ContainSingle().Which.Host.Should().Be("a.example");
        second.Should().ContainSingle().Which.Host.Should().Be("b.example");
    }

    [Fact]
    public async Task ParallelClaimsOnOneHostNeverGiveTwoUrls()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        for (var page = 0; page < 8; page++)
        {
            await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://a.example/{page}", depth: page);
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Claim()));

        results.SelectMany(r => r).Should().ContainSingle();
    }

    [Fact]
    public async Task AWaitBookedBeyondTheHoldSurvivesTheCompletion()
    {
        await TwoUrlsOnOneHost();
        var first = (await Claim()).Single();
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "Hosts" SET "NextAllowedAt" = @until WHERE "Host" = 'a.example' RETURNING 1""",
            new { until = TestData.Now.AddMinutes(10) });

        await Finish(first, Fast());
        _time.Advance(TimeSpan.FromMinutes(9));

        (await Claim()).Should().BeEmpty();
    }

    [Fact]
    public async Task JobProgressShowsNoNextRequestWhileARequestIsInFlightAndOneAfterTheCompletion()
    {
        var job = await TwoUrlsOnOneHost();
        var reader = new PostgresJobProgressReader(postgres.DataSource);
        var first = (await Claim()).Single();

        var inFlight = (await reader.Read(job, default))!;
        await Finish(first, Fast());
        var waiting = (await reader.Read(job, default))!;

        inFlight.InFlight.Should().Be(1);
        inFlight.NextAllowedAt.Should().BeNull();
        waiting.InFlight.Should().Be(0);
        waiting.NextAllowedAt.Should().Be(TestData.Now.AddSeconds(1));
    }
}
