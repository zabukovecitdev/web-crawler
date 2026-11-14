using FluentAssertions;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

/// <summary>UNVERIFIED: written without Docker. Run with <c>--filter "Category=Integration"</c>.</summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresJobProgressReaderTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresJobProgressReader Reader => new(postgres.DataSource);

    private async Task<int> Fetched(int job, string url, int status, long responseMs, string urlStatus = "Idle", string? failReason = null)
    {
        var urlId = await PostgresTestData.InsertUrl(postgres.DataSource, job, url, status: urlStatus);
        var fetchId = await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ResponseTimeMs")
            VALUES (@urlId, @at, @status, 'text/html', @responseMs) RETURNING "Id"
            """,
            new { urlId, at = TestData.Now, status, responseMs });
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetchId, "FailReason" = @failReason WHERE "Id" = @urlId RETURNING "Id"
            """,
            new { fetchId, urlId, failReason });
        return urlId;
    }

    [Fact]
    public async Task ReadReturnsNullForAMissingJob() => (await Reader.Read(999, default)).Should().BeNull();

    [Fact]
    public async Task ReadCountsFrontierStatesAndFetchStatistics()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, maxUrls: 50);
        await Fetched(job, "https://a.example/ok1", 200, 100);
        await Fetched(job, "https://a.example/ok2", 200, 300);
        await Fetched(job, "https://a.example/moved", 301, 50, "Disabled", "redirect");
        await Fetched(job, "https://a.example/missing", 404, 80, "Disabled", "http-404");
        await Fetched(job, "https://a.example/boom", 503, 20, "Disabled", "max-failures");
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/waiting");
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/leased", status: "InFlight", depth: 2);

        var data = (await Reader.Read(job, default))!;

        data.Status.Should().Be("Running");
        data.MaxUrls.Should().Be(50);
        data.Crawled.Should().Be(2);
        data.Failed.Should().Be(2);
        data.Queued.Should().Be(1);
        data.InFlight.Should().Be(1);
        data.CurrentUrl.Should().Be("https://a.example/leased");
        data.CurrentDepth.Should().Be(2);
        data.Fetches.Should().Be(5);
        data.MaxResponseMs.Should().Be(300);
        data.AvgResponseMs.Should().BeApproximately(110, 0.001);
        (data.Ok, data.Redirect, data.ClientError, data.ServerError).Should().Be((2, 1, 1, 1));
    }

    [Fact]
    public async Task ReadTakesTheDelayOfTheHostAndNamesRobotsAsTheSource()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/");

        var withoutRobots = (await Reader.Read(job, default))!;
        await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "RobotsTxt" ("Host", "Content", "ParsedRules", "FetchedAt", "ExpiresAt", "CrawlDelayMs")
            VALUES ('a.example', 'User-agent: *', '[]'::jsonb, now(), now() + interval '1 day', 4000) RETURNING 1
            """);
        await PostgresTestData.Scalar<int>(
            postgres.DataSource, """UPDATE "Hosts" SET "CrawlDelayMs" = 4000 WHERE "Host" = 'a.example' RETURNING 1""");
        var withRobots = (await Reader.Read(job, default))!;

        (withoutRobots.CrawlDelayMs, withoutRobots.DelayFromRobots).Should().Be((1000, false));
        (withRobots.CrawlDelayMs, withRobots.DelayFromRobots).Should().Be((4000, true));
        withRobots.NextAllowedAt.Should().NotBeNull();
    }

    [Fact]
    public async Task ReadIgnoresOtherJobs()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var other = await PostgresTestData.InsertJob(postgres.DataSource);
        await Fetched(other, "https://a.example/elsewhere", 200, 10);

        var data = (await Reader.Read(job, default))!;

        (data.Crawled, data.Fetches, data.Queued).Should().Be((0, 0, 0));
        data.AvgResponseMs.Should().BeNull();
        data.CurrentUrl.Should().BeNull();
    }
}
