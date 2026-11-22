using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresPacingTests(PostgresFixture postgres) : IAsyncLifetime
{
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(1);
    private static readonly TimeSpan Hold = TimeSpan.FromSeconds(30);
    private const string HostRow = """SELECT * FROM "Hosts" WHERE "Host" = 'a.example'""";

    private readonly FakeTimeProvider _time = new(TestData.Now);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresUrlFrontier Frontier() => new(postgres.DataSource, _time);

    private FetchRecorder Recorder() =>
        new(new FakeContentStore(), Frontier(), new PolitenessPolicy(TimeSpan.FromSeconds(60)), _time);

    private async Task<ClaimedUrl> ClaimOne(string url = "https://a.example/")
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, url);
        return (await Frontier().Claim(1, Lease, Hold, true, default)).Single();
    }

    private static FetchRecord Fetch() => new() { FetchedAt = TestData.Now, StatusCode = 200, ResponseTimeMs = 42, FinalUrl = "https://a.example/" };

    private async Task SetHost(string setClause, object? parameters = null) =>
        await PostgresTestData.Scalar<int>(postgres.DataSource, $"""UPDATE "Hosts" SET {setClause} WHERE "Host" = 'a.example' RETURNING 1""", parameters);

    [Fact]
    public async Task ClaimReturnsThePacingStateOfTheHost()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");
        await SetHost("\"AvgResponseMs\" = 420.5, \"ConsecutiveFailures\" = 3");

        var claimed = (await Frontier().Claim(1, Lease, Hold, true, default)).Single();

        (claimed.AvgResponseMs, claimed.ConsecutiveFailures).Should().Be((420.5, 3));
    }

    [Fact]
    public async Task CompleteStoresThePacingInTheSameStepAsTheFetch()
    {
        var url = await ClaimOne();
        var update = new Succeeded(TimeSpan.FromDays(1)).ToUpdate(url, TestData.Now) with
        {
            Pacing = new HostPacingUpdate(TimeSpan.FromSeconds(5), 120.5, 0)
        };

        await Frontier().Complete(url, update, Fetch(), default);

        var host = await PostgresTestData.Row(postgres.DataSource, HostRow);
        ((double)host.AvgResponseMs).Should().Be(120.5);
        ((int)host.ConsecutiveFailures).Should().Be(0);
        ((int)host.LastDelayMs).Should().Be(5000);
        ((DateTimeOffset)host.NextAllowedAt).Should().Be(TestData.Now.AddSeconds(5));
    }

    [Fact]
    public async Task CompleteKeepsALongerWaitThatIsAlreadyBooked()
    {
        var url = await ClaimOne();
        await SetHost("\"NextAllowedAt\" = @until", new { until = TestData.Now.AddHours(1) });
        var update = new RetryLater("http-500", TimeSpan.FromMinutes(1)).ToUpdate(url, TestData.Now) with
        {
            Pacing = new HostPacingUpdate(TimeSpan.FromSeconds(2), null, 3)
        };

        await Frontier().Complete(url, update, Fetch(), default);

        var host = await PostgresTestData.Row(postgres.DataSource, HostRow);
        ((DateTimeOffset)host.NextAllowedAt).Should().Be(TestData.Now.AddHours(1));
        ((int)host.ConsecutiveFailures).Should().Be(3);
        ((object?)host.AvgResponseMs).Should().BeNull();
    }

    [Fact]
    public async Task CompleteWithTheClockMovedBooksTheDelayFromTheTimeOfTheCompletion()
    {
        var url = await ClaimOne();
        _time.Advance(TimeSpan.FromSeconds(10));
        var update = new Succeeded(TimeSpan.FromDays(1)).ToUpdate(url, _time.GetUtcNow()) with
        {
            Pacing = new HostPacingUpdate(TimeSpan.FromSeconds(30), 6000, 0)
        };

        await Frontier().Complete(url, update, Fetch(), default);

        var host = await PostgresTestData.Row(postgres.DataSource, HostRow);
        ((DateTimeOffset)host.NextAllowedAt).Should().Be(TestData.Now.AddSeconds(40));
    }

    [Fact]
    public async Task ARobotsDisallowedUrlLeavesThePacingOfTheHostAlone()
    {
        var url = await ClaimOne();
        await SetHost("\"AvgResponseMs\" = 250, \"ConsecutiveFailures\" = 2, \"LastDelayMs\" = 4000");

        await Recorder().RecordDisallowed(url, default);

        var host = await PostgresTestData.Row(postgres.DataSource, HostRow);
        ((double)host.AvgResponseMs).Should().Be(250);
        ((int)host.ConsecutiveFailures).Should().Be(2);
        ((int)host.LastDelayMs).Should().Be(4000);
    }

    [Fact]
    public async Task ABlockedAddressLeavesThePacingOfTheHostAlone()
    {
        var url = await ClaimOne();
        await SetHost("\"AvgResponseMs\" = 250, \"ConsecutiveFailures\" = 2, \"LastDelayMs\" = 4000");
        var response = FetchResponse.BlockedAddress(15);

        await Recorder().Record(url, response, OutcomeClassifier.Classify(response, 0), default);

        var host = await PostgresTestData.Row(postgres.DataSource, HostRow);
        ((double)host.AvgResponseMs).Should().Be(250);
        ((int)host.ConsecutiveFailures).Should().Be(2);
        ((int)host.LastDelayMs).Should().Be(4000);
    }

    [Fact]
    public async Task ARecordedTimeoutRaisesTheFailureCountAndDoublesTheDelay()
    {
        var url = await ClaimOne();
        var response = new FetchResponse { StatusCode = 0, Error = "timeout", ResponseTimeMs = 20_000 };

        var recorded = await Recorder().Record(url, response, OutcomeClassifier.Classify(response, 0), default);

        recorded.Delay.Should().Be(TimeSpan.FromSeconds(2));
        var host = await PostgresTestData.Row(postgres.DataSource, HostRow);
        ((int)host.ConsecutiveFailures).Should().Be(1);
        ((object?)host.AvgResponseMs).Should().BeNull();
        ((DateTimeOffset)host.NextAllowedAt).Should().Be(TestData.Now.AddSeconds(2));
    }

    [Fact]
    public async Task ASuccessAfterFailuresResetsTheCount()
    {
        var url = await ClaimOne();
        await SetHost("\"ConsecutiveFailures\" = 4, \"AvgResponseMs\" = 100");
        var claimed = url with { ConsecutiveFailures = 4, AvgResponseMs = 100 };
        var response = TestData.Response(200) with { ResponseTimeMs = 100 };

        await Recorder().Record(claimed, response, OutcomeClassifier.Classify(response, 0), default);

        var host = await PostgresTestData.Row(postgres.DataSource, HostRow);
        ((int)host.ConsecutiveFailures).Should().Be(0);
        ((double)host.AvgResponseMs).Should().BeApproximately(100, 0.001);
    }

    [Fact]
    public async Task ClaimsInParallelNeverGiveTwoUrlsOfOneHost()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, allowedHosts: "[]");
        for (var host = 0; host < 3; host++)
        {
            for (var page = 0; page < 3; page++)
            {
                await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://h{host}.example/{page}");
            }
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 6).Select(_ => Frontier().Claim(10, Lease, Hold, true, default)));

        var hosts = results.SelectMany(r => r).Select(u => u.Host).ToList();
        hosts.Should().OnlyHaveUniqueItems();
        hosts.Should().NotBeEmpty();
    }

    [Fact]
    public async Task JobProgressReportsTheEffectiveDelayAsTheLargerOfBaseAndAdaptive()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");
        var reader = new PostgresJobProgressReader(postgres.DataSource);

        var before = (await reader.Read(job, default))!;
        await SetHost("\"LastDelayMs\" = 8000");
        var adaptive = (await reader.Read(job, default))!;
        await SetHost("\"LastDelayMs\" = 200, \"CrawlDelayMs\" = 3000");
        var baseWins = (await reader.Read(job, default))!;

        (before.CrawlDelayMs, adaptive.CrawlDelayMs, baseWins.CrawlDelayMs).Should().Be((1000, 8000, 3000));
    }
}
