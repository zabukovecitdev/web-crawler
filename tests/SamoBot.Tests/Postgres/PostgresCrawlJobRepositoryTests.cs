using Dapper;
using FluentAssertions;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresCollection.Name)]
[Trait("Category", "Integration")]
public class PostgresCrawlJobRepositoryTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private PostgresCrawlJobRepository Repository() => new(postgres.DataSource);

    private static CrawlJob NewJob() => CrawlJob.Create(
        new NewCrawlJob("owner-1", ["https://a.example/", "https://b.example/x"], 3, 100, true, false),
        TestData.Now).Value;

    [Fact]
    public async Task InsertAndGet_RoundTripAllFieldsIncludingJsonListsAndStatus()
    {
        var id = await Repository().Insert(NewJob(), default);

        var job = await Repository().Get(id, default);

        job.Should().NotBeNull();
        job!.Id.Should().Be(id);
        job.OwnerUserId.Should().Be("owner-1");
        job.SeedUrls.Should().Equal("https://a.example/", "https://b.example/x");
        job.AllowedHosts.Should().Equal("a.example", "b.example");
        (job.MaxDepth, job.MaxUrls, job.UseJsRendering, job.RespectRobots).Should().Be((3, 100, true, false));
        job.Status.Should().Be(CrawlJobStatus.Pending);
        job.CreatedAt.Should().Be(TestData.Now);
    }

    [Fact]
    public async Task Save_StoresTheTransitionsOfALoadedJob()
    {
        var id = await Repository().Insert(NewJob(), default);
        var job = (await Repository().Get(id, default))!;

        job.Start(TestData.Now.AddMinutes(5)).IsSuccess.Should().BeTrue();
        await Repository().Save(job, default);

        var reloaded = (await Repository().Get(id, default))!;
        reloaded.Status.Should().Be(CrawlJobStatus.Running);
        reloaded.StartedAt.Should().Be(TestData.Now.AddMinutes(5));
        reloaded.UpdatedAt.Should().Be(TestData.Now.AddMinutes(5));
    }

    [Fact]
    public async Task Get_OfAMissingJob_ReturnsNull()
    {
        (await Repository().Get(999, default)).Should().BeNull();
    }

    [Fact]
    public async Task ListRecent_ReturnsNewestFirstWithinTheLimit()
    {
        var first = await Repository().Insert(NewJob(), default);
        var second = await Repository().Insert(NewJob(), default);
        await Repository().Insert(NewJob(), default);

        var jobs = await Repository().ListRecent(2, default);

        jobs.Should().HaveCount(2);
        jobs[1].Id.Should().BeGreaterThan(first).And.Be(second);
    }

    private async Task<int> RunningJob(string status = "Running") =>
        await PostgresTestData.InsertJob(postgres.DataSource, status);

    private async Task<int> Url(int job, string path, string status = "Idle", bool crawled = true, int failCount = 0)
    {
        var id = await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://a.example/{path}", status: status);
        await using var connection = await postgres.DataSource.OpenConnectionAsync();
        await connection.ExecuteAsync(
            """UPDATE "DiscoveredUrls" SET "LastCrawlAt" = @crawled, "FailCount" = @failCount WHERE "Id" = @id""",
            new { id, failCount, crawled = crawled ? TestData.Now : (DateTimeOffset?)null });
        return id;
    }

    private async Task<IReadOnlyList<int>> Complete() =>
        await Repository().CompleteFinishedJobs(TestData.Now.AddHours(1), default);

    [Fact]
    public async Task CompleteFinishedJobs_CompletesAJobWhoseUrlsAreAllDone()
    {
        var job = await RunningJob();
        var done = await Url(job, "a");
        await PostgresTestData.InsertFetch(postgres.DataSource, done, parsed: true);
        await Url(job, "b", status: "Disabled", crawled: false);

        (await Complete()).Should().Equal(job);

        var row = await PostgresTestData.Row(postgres.DataSource, """SELECT * FROM "CrawlJobs" WHERE "Id" = @job""", new { job });
        ((string)row.Status).Should().Be("Completed");
        ((DateTimeOffset)row.CompletedAt).Should().Be(TestData.Now.AddHours(1));
    }

    [Theory]
    [InlineData("InFlight", true, 0)]
    [InlineData("Idle", false, 0)]
    [InlineData("Idle", true, 2)]
    public async Task CompleteFinishedJobs_DoesNotCompleteAJobWithPendingWork(string status, bool crawled, int failCount)
    {
        var job = await RunningJob();
        await Url(job, "a", status, crawled, failCount);

        (await Complete()).Should().BeEmpty();
    }

    [Fact]
    public async Task CompleteFinishedJobs_DoesNotCompleteAJobWithAnUnparsedPage()
    {
        var job = await RunningJob();
        var done = await Url(job, "a");
        await PostgresTestData.InsertFetch(postgres.DataSource, done, parsed: false);

        (await Complete()).Should().BeEmpty();
    }

    [Fact]
    public async Task CompleteFinishedJobs_IgnoresJobsThatAreNotRunning()
    {
        await Url(await RunningJob("Paused"), "a");

        (await Complete()).Should().BeEmpty();
    }
}
