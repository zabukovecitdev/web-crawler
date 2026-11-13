using FluentAssertions;
using Meilisearch;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters;
using SamoBot.Core;
using SamoBot.Core.Common;
using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;
using SamoBot.Core.Indexing;
using SamoBot.Core.Jobs;
using SamoBot.Core.Parsing;
using SamoBot.Core.Ports;
using SamoBot.Tests.Fakes;
using SamoBot.Tests.Postgres;

namespace SamoBot.Tests.EndToEnd;

/// <summary>
/// A whole crawl with real PostgreSQL, RustFS and Meilisearch and a real HTTP stub site.
/// The clock is fake, so the retry backoff of the failing page costs no real time.
/// </summary>
[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class CrawlJobTests(PostgresFixture postgres, InfrastructureFixture infrastructure)
    : IClassFixture<InfrastructureFixture>, IAsyncLifetime
{
    private readonly FakeTimeProvider _time = new(TestData.Now);
    private readonly string _indexName = $"e2e_{Guid.NewGuid():N}";

    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private ServiceProvider Build()
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Database:ConnectionString"] = postgres.ConnectionString,
            ["MinIO:Endpoint"] = infrastructure.StorageHost,
            ["MinIO:Port"] = infrastructure.StoragePort.ToString(System.Globalization.CultureInfo.InvariantCulture),
            ["MinIO:AccessKey"] = InfrastructureFixture.StorageUser,
            ["MinIO:SecretKey"] = InfrastructureFixture.StorageUser,
            ["MinIO:BucketName"] = "e2e-content",
            ["Meilisearch:Host"] = infrastructure.MeilisearchUrl,
            ["Meilisearch:ApiKey"] = InfrastructureFixture.MeilisearchKey,
            ["Meilisearch:IndexName"] = _indexName
        }).Build();

        return new ServiceCollection()
            .AddLogging()
            .AddSingleton<TimeProvider>(_time)
            .AddSamoBotCore()
            .AddSamoBotAdapters(configuration)
            .BuildServiceProvider();
    }

    /// <summary>Runs each handler once. Returns the number of crawled, parsed and indexed items.</summary>
    private static async Task<int> RunHandlersOnce(IServiceProvider root)
    {
        await using var scope = root.CreateAsyncScope();
        var services = scope.ServiceProvider;
        var worked = await services.GetRequiredService<CrawlBatchHandler>().HandleBatch(default);
        worked += await services.GetRequiredService<ParseBatchHandler>().HandleBatch(default);
        worked += await services.GetRequiredService<IndexBatchHandler>().HandleBatch(default);
        await services.GetRequiredService<JobCompletionHandler>().HandleBatch(default);
        return worked;
    }

    private async Task RunUntilCompleted(ServiceProvider provider, int jobId)
    {
        var jobs = provider.GetRequiredService<ICrawlJobRepository>();
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            var worked = await RunHandlersOnce(provider);
            if ((await jobs.Find(jobId, default))!.Status == CrawlJobStatus.Completed)
            {
                return;
            }

            // Busy: move just past the host delay. Idle: jump to the next retry.
            _time.Advance(worked > 0 ? TimeSpan.FromMilliseconds(1100) : TimeSpan.FromSeconds(30));
        }

        throw new TimeoutException("The job did not complete within 60 seconds.");
    }

    private Task<IReadOnlyList<T>> Rows<T>(string sql, object? parameters = null) =>
        PostgresTestData.Rows<T>(postgres.DataSource, sql, parameters);

    [Fact]
    public async Task CrawlingTheStubSiteFollowsTheRulesOfTheDesign()
    {
        await using var site = await StubSite.Start();
        await using var provider = Build();
        var service = provider.GetRequiredService<CrawlJobService>();
        var created = await service.Create(new NewCrawlJob(null, [$"{site.BaseUrl}/"], null, null, false, true), default);
        var jobId = created.Value.Id;
        (await service.Start(jobId, default)).IsSuccess.Should().BeTrue();

        await RunUntilCompleted(provider, jobId);

        var expected = StubSite.SuccessfulPaths.Select(path => $"{site.BaseUrl}{path}").ToList();
        await AssertEachAllowedPageHasOneSuccessfulFetch(expected);
        await AssertTheDisallowedPageIsNeverFetched(site);
        await AssertTheFailingPageEndsDisabled(site);
        await AssertFetchesToTheHostAreAtLeastOneSecondApart();
        await AssertEveryFetchedPageIsInTheIndex(expected.Count);
    }

    private async Task AssertEachAllowedPageHasOneSuccessfulFetch(List<string> expected)
    {
        var fetches = await Rows<(string Url, long Count)>(
            """
            SELECT u."NormalizedUrl" AS "Url", count(*) AS "Count"
            FROM "UrlFetches" f JOIN "DiscoveredUrls" u ON u."Id" = f."DiscoveredUrlId"
            WHERE f."StatusCode" = 200 GROUP BY u."NormalizedUrl"
            """);

        fetches.Select(f => f.Url).Should().BeEquivalentTo(expected);
        fetches.Should().OnlyContain(f => f.Count == 1);
    }

    private async Task AssertTheDisallowedPageIsNeverFetched(StubSite site)
    {
        var row = await PostgresTestData.Row(
            postgres.DataSource, """SELECT * FROM "DiscoveredUrls" WHERE "NormalizedUrl" LIKE '%/private/secret'""");
        ((string)row.Status).Should().Be("Disabled");
        ((string)row.FailReason).Should().Be("robots");
        var fetches = await PostgresTestData.Scalar<long>(
            postgres.DataSource, """SELECT count(*) FROM "UrlFetches" WHERE "DiscoveredUrlId" = @id""", new { id = (int)row.Id });
        fetches.Should().Be(0);
        site.Requested.Should().NotContain("/private/secret");
    }

    private async Task AssertTheFailingPageEndsDisabled(StubSite site)
    {
        var row = await PostgresTestData.Row(
            postgres.DataSource, """SELECT * FROM "DiscoveredUrls" WHERE "NormalizedUrl" LIKE '%/boom'""");
        ((string)row.Status).Should().Be("Disabled");
        ((string)row.FailReason).Should().Be("max-failures");
        site.Requested.Count(path => path == "/boom").Should().Be(OutcomeClassifier.MaxFailures);
    }

    private async Task AssertFetchesToTheHostAreAtLeastOneSecondApart()
    {
        var times = await Rows<DateTime>("""SELECT "FetchedAt" FROM "UrlFetches" ORDER BY "FetchedAt" """);

        times.Zip(times.Skip(1), (earlier, later) => later - earlier)
            .Should().OnlyContain(gap => gap >= TimeSpan.FromSeconds(1));
    }

    private async Task AssertEveryFetchedPageIsInTheIndex(int pageCount)
    {
        var client = new MeilisearchClient(infrastructure.MeilisearchUrl, InfrastructureFixture.MeilisearchKey);
        var stats = await client.Index(_indexName).GetStatsAsync();
        stats.NumberOfDocuments.Should().Be(pageCount);

        await using var provider = Build();
        var hits = await provider.GetRequiredService<ISearchQuery>().Search("Page 7", 5, default);
        hits.Should().Contain(hit => hit.Title == "Page 7");
    }
}
