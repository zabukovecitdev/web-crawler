using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresAnalyticsTests(PostgresFixture postgres) : IAsyncLifetime
{
    private static readonly DateTimeOffset Now = new(2026, 1, 10, 12, 30, 0, TimeSpan.Zero);

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresAnalyticsReader Analytics => new(postgres.DataSource, new FakeTimeProvider(Now));

    private Task<int> Fetch(
        int urlId,
        TimeSpan age,
        int? status,
        long? responseMs = 100,
        string? contentType = "text/html; charset=utf-8",
        long length = 1000) =>
        PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ContentLength", "ResponseTimeMs")
            VALUES (@urlId, @at, @status, @contentType, @length, @responseMs) RETURNING "Id"
            """,
            new { urlId, at = Now - age, status = status ?? 0, contentType, length, responseMs });

    private async Task<(int A1, int A2, int B1)> Seed()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var a1 = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/1");
        var a2 = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/2");
        var b1 = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://b.example/1");
        await Fetch(a1, TimeSpan.FromHours(1), 200, 100);
        await Fetch(a1, TimeSpan.FromHours(30), 200, 300);
        await Fetch(a2, TimeSpan.FromHours(2), 404, 200, "text/plain");
        await Fetch(b1, TimeSpan.FromDays(10), 500, 400, "application/pdf");
        return (a1, a2, b1);
    }

    [Fact]
    public async Task OverviewCountsRequestsPagesAndFailuresInTheRange()
    {
        await Seed();

        var all = await Analytics.Overview(AnalyticsRange.All, default);
        var day = await Analytics.Overview(AnalyticsRange.Day, default);

        all.Hosts.Should().Be(2);
        all.Pages.Should().Be(3);
        all.Requests.Should().Be(4);
        all.Bytes.Should().Be(4000);
        all.FailedPages.Should().Be(2);
        all.FailureRate.Should().BeApproximately(0.5, 0.0001);
        all.P50ResponseMs.Should().BeApproximately(250, 0.0001);
        all.MaxResponseMs.Should().Be(400);
        all.From.Should().BeNull();
        all.To.Should().Be(Now);
        day.Requests.Should().Be(2);
        day.Pages.Should().Be(2);
        day.Hosts.Should().Be(1);
        day.From.Should().Be(Now.AddHours(-24));
    }

    [Fact]
    public async Task OverviewOfNothingHasNullPercentilesAndZeroRate()
    {
        var overview = await Analytics.Overview(AnalyticsRange.All, default);

        overview.Requests.Should().Be(0);
        overview.FailureRate.Should().Be(0);
        overview.P50ResponseMs.Should().BeNull();
        overview.P95ResponseMs.Should().BeNull();
        overview.MaxResponseMs.Should().BeNull();
    }

    [Fact]
    public async Task FetchesWithoutStatusCountAsFailures()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var url = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/x");
        await Fetch(url, TimeSpan.FromHours(1), status: null, responseMs: null);

        var overview = await Analytics.Overview(AnalyticsRange.All, default);

        overview.FailureRate.Should().Be(1);
        overview.FailedPages.Should().Be(1);
    }

    [Fact]
    public async Task HostsAreSortedByPagesWithCrawlDelay()
    {
        await Seed();
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """UPDATE "Hosts" SET "CrawlDelayMs" = 2500 WHERE "Host" = 'a.example' RETURNING 1""");

        var hosts = await Analytics.Hosts(AnalyticsRange.All, default);

        hosts.Select(h => h.Host).Should().Equal("a.example", "b.example");
        hosts[0].Pages.Should().Be(2);
        hosts[0].Requests.Should().Be(3);
        hosts[0].CrawlDelayMs.Should().Be(2500);
        hosts[0].FailureRate.Should().BeApproximately(1.0 / 3, 0.0001);
        hosts[1].P95ResponseMs.Should().Be(400);
        (await Analytics.Hosts(AnalyticsRange.Week, default)).Select(h => h.Host).Should().Equal("a.example");
    }

    [Fact]
    public async Task StatusCodesGroupByClassAndFilterByHost()
    {
        var (a1, _, b1) = await Seed();
        await Fetch(a1, TimeSpan.FromHours(3), 301);
        await Fetch(b1, TimeSpan.FromHours(3), 503);

        var all = await Analytics.StatusCodes(AnalyticsRange.All, null, default);
        var b = await Analytics.StatusCodes(AnalyticsRange.All, "B.example", default);

        all.Should().Be(new StatusClassCounts { Ok = 2, Redirect = 1, ClientError = 1, ServerError = 2 });
        b.Should().Be(new StatusClassCounts { ServerError = 2 });
    }

    [Fact]
    public async Task ContentTypesAreNormalizedCountedAndMarkedStored()
    {
        await Seed();

        var types = await Analytics.ContentTypes(AnalyticsRange.All, null, default);

        types[0].Should().Be(new AnalyticsContentType("text/html", 2, true));
        types.Select(t => t.ContentType).Should().BeEquivalentTo("text/html", "text/plain", "application/pdf");
        types.Single(t => t.ContentType == "application/pdf").Stored.Should().BeFalse();
        (await Analytics.ContentTypes(AnalyticsRange.All, "b.example", default)).Should().ContainSingle();
    }

    [Fact]
    public async Task ContentTypesKeepTheTopTen()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var url = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/x");
        for (var i = 0; i < 12; i++)
        {
            await Fetch(url, TimeSpan.FromHours(1), 200, contentType: $"application/x-{i:00}");
        }

        (await Analytics.ContentTypes(AnalyticsRange.All, null, default)).Should().HaveCount(10);
    }

    [Fact]
    public async Task HourlyTimeseriesFillsGapsWithZero()
    {
        await Seed();

        var points = await Analytics.Timeseries(AnalyticsRange.Day, null, AnalyticsBucket.Hour, default);

        points.Should().HaveCount(25);
        points[^1].Bucket.Should().Be(new DateTimeOffset(2026, 1, 10, 12, 0, 0, TimeSpan.Zero));
        points.Select(p => p.Bucket).Should().BeInAscendingOrder();
        points.Sum(p => p.Requests).Should().Be(2);
        points.Single(p => p.Bucket.Hour == 11 && p.Bucket.Day == 10).Requests.Should().Be(1);
        points.Count(p => p.Requests == 0).Should().Be(23);
    }

    [Fact]
    public async Task DailyTimeseriesOfAllStartsAtTheOldestFetchAndFiltersByHost()
    {
        await Seed();

        var all = await Analytics.Timeseries(AnalyticsRange.All, null, AnalyticsBucket.Day, default);
        var a = await Analytics.Timeseries(AnalyticsRange.All, "a.example", AnalyticsBucket.Day, default);

        all[0].Bucket.Should().Be(new DateTimeOffset(2025, 12, 31, 0, 0, 0, TimeSpan.Zero));
        all[^1].Bucket.Should().Be(new DateTimeOffset(2026, 1, 10, 0, 0, 0, TimeSpan.Zero));
        all.Should().HaveCount(11);
        all.Sum(p => p.Requests).Should().Be(4);
        a.Sum(p => p.Requests).Should().Be(3);
    }

    [Fact]
    public async Task TimeseriesOfNothingIsEmptyForAllAndZeroFilledForARange()
    {
        (await Analytics.Timeseries(AnalyticsRange.All, null, AnalyticsBucket.Day, default)).Should().BeEmpty();

        var week = await Analytics.Timeseries(AnalyticsRange.Week, null, AnalyticsBucket.Day, default);

        week.Should().HaveCount(8);
        week.Should().OnlyContain(p => p.Requests == 0);
    }
}
