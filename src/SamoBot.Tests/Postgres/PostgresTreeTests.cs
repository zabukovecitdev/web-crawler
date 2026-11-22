using FluentAssertions;
using Microsoft.Extensions.Time.Testing;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresTreeTests(PostgresFixture postgres) : IAsyncLifetime
{
    private readonly FakeTimeProvider _time = new(TestData.Now);

    public async Task InitializeAsync()
    {
        await postgres.Reset();
        await PostgresTestData.Execute(postgres.DataSource, """TRUNCATE "SitemapUrls", "SitemapFiles", "SitemapRequests" RESTART IDENTITY CASCADE""");
    }

    public Task DisposeAsync() => Task.CompletedTask;

    private async Task Sitemap(params (string Path, string? Modified)[] urls)
    {
        await PostgresTestData.Execute(postgres.DataSource, """INSERT INTO "SitemapFiles" ("Host", "Url", "Kind", "UrlCount", "FetchedAt") VALUES ('a.example', 'https://a.example/s.xml', 'UrlSet', @n, now())""", new { n = urls.Length });
        foreach (var (path, modified) in urls)
        {
            await PostgresTestData.Execute(
                postgres.DataSource,
                """
                INSERT INTO "SitemapUrls" ("Host", "Loc", "NormalizedLoc", "LastModified", "SitemapFileId")
                VALUES ('a.example', @loc, @loc, @modified::timestamptz, (SELECT max("Id") FROM "SitemapFiles"))
                """,
                new { loc = "https://a.example" + path, modified });
        }
    }

    private Task<IReadOnlyList<FolderGroup>> Folders(string parent) =>
        new PostgresSitemapPlanReader(postgres.DataSource).Folders("a.example", parent, default);

    [Fact]
    public async Task TheSitemapIsGroupedByTheFirstTwoPathSegmentsBelowTheParent()
    {
        await Sitemap(("/", null), ("/guide/", null), ("/guide/install", "2026-11-18Z"), ("/guide/quickstart", "2026-10-02Z"), ("/blog/2026/a", null), ("/blog/2026/b", null), ("/blog/2025/a", null), ("/changelog", null));

        var groups = await Folders("/");

        groups.Select(group => (group.First, group.Second, group.Counts.Pages)).Should().BeEquivalentTo(
        [
            ("", "", 1), ("guide/", "", 1), ("guide/", "install", 1), ("guide/", "quickstart", 1),
            ("blog/", "2026/", 2), ("blog/", "2025/", 1), ("changelog", "", 1)
        ]);
        var install = groups.Single(group => group.Second == "install").Counts;
        install.OldestModified.Should().Be(new DateTimeOffset(2026, 11, 18, 0, 0, 0, TimeSpan.Zero));
    }

    [Fact]
    public async Task ALowerParentOnlyCountsWhatLivesBelowIt()
    {
        await Sitemap(("/blog/2026/a", null), ("/blog/2026/b", null), ("/blog/2025/a", null), ("/guide/x", null), ("/blog/", null));

        var groups = await Folders("/blog/");

        groups.Select(group => (group.First, group.Second, group.Counts.Pages)).Should().BeEquivalentTo(
            [("", "", 1), ("2026/", "a", 1), ("2026/", "b", 1), ("2025/", "a", 1)]);
    }

    [Fact]
    public async Task QueryStringsDoNotChangeThePathOfAUrl()
    {
        await Sitemap(("/search?q=1", null), ("/search?q=2", null));

        (await Folders("/")).Should().ContainSingle().Which.Counts.Pages.Should().Be(2);
    }

    [Fact]
    public async Task TheJobTreeCountsEachFolderByState()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var done = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/blog/2026/a");
        var queued = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/blog/2026/b");
        var failed = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/blog/2026/c", status: "Disabled");
        var stopped = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/blog/2025/a", status: "Disabled");
        var fetching = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/guide", status: "InFlight");
        var fetchId = await PostgresTestData.InsertFetch(postgres.DataSource, done);
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetchId, "LastCrawlAt" = now() WHERE "Id" = @done""", new { fetchId, done });
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "DiscoveredUrls" SET "FailReason" = 'http-500' WHERE "Id" = @failed; UPDATE "DiscoveredUrls" SET "FailReason" = 'unselected' WHERE "Id" = @stopped""", new { failed, stopped });

        var groups = await new PostgresJobTreeReader(postgres.DataSource).Folders(job, "/blog/", default);

        var total = groups.Aggregate(FolderCounts.None, (sum, group) => sum.Plus(group.Counts));
        (total.Pages, total.Done, total.Queued, total.Failed, total.Stopped, total.Fetching).Should().Be((4, 1, 1, 1, 1, 0));
        queued.Should().BeGreaterThan(0);
        fetching.Should().BeGreaterThan(0);
        (await new PostgresJobTreeReader(postgres.DataSource).Folders(job, "/", default)).Sum(group => group.Counts.Fetching).Should().Be(1);
    }

    [Fact]
    public async Task SeedingFromTheSitemapInsertsOnlyTheTickedPathsAtDepthZero()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource, status: "Pending");
        await PostgresTestData.Execute(
            postgres.DataSource,
            """UPDATE "CrawlJobs" SET "PathRules" = '[{"kind":"Exclude","prefix":"/blog/"},{"kind":"Include","prefix":"/blog/2026/"}]', "RenderMode" = 'Always' WHERE "Id" = @job""",
            new { job });
        await PostgresTestData.Execute(postgres.DataSource, """INSERT INTO "Hosts" ("Host", "NextAllowedAt") VALUES ('a.example', now()) ON CONFLICT DO NOTHING""");
        await Sitemap(("/guide/a", null), ("/blog/2016/x", null), ("/blog/2026/y", null), ("/", null));

        var count = await new PostgresSitemapSeeder(postgres.DataSource, _time).SeedFromSitemap(job, "a.example", default);
        var again = await new PostgresSitemapSeeder(postgres.DataSource, _time).SeedFromSitemap(job, "a.example", default);

        (count, again).Should().Be((3, 0));
        var rows = await PostgresTestData.Rows<string>(postgres.DataSource, """SELECT "Url" || ' ' || "Depth" || ' ' || "UseJsRendering" FROM "DiscoveredUrls" WHERE "CrawlJobId" = @job ORDER BY "Url" """, new { job });
        rows.Should().Equal("https://a.example/ 0 true", "https://a.example/blog/2026/y 0 true", "https://a.example/guide/a 0 true");
    }

    [Fact]
    public async Task ApplyingRulesStopsQueuedUrlsOfUntickedFoldersAndKeepsFetchedOnes()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var queued = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/events/calendar/2026-01-01");
        var fetched = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/events/calendar/2026-01-02");
        var other = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/guide/a");
        var fetchId = await PostgresTestData.InsertFetch(postgres.DataSource, fetched);
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetchId, "LastCrawlAt" = now() WHERE "Id" = @fetched""", new { fetchId, fetched });
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "CrawlJobs" SET "PathRules" = '[{"kind":"Exclude","prefix":"/events/calendar/"}]' WHERE "Id" = @job""", new { job });
        var applier = new PostgresPathRuleApplier(postgres.DataSource, _time);

        var stopped = await applier.Apply(job, default);
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "CrawlJobs" SET "PathRules" = '[]' WHERE "Id" = @job""", new { job });
        var resumed = await applier.Apply(job, default);

        stopped.Should().Be(new PathRuleEffect(1, 0));
        resumed.Should().Be(new PathRuleEffect(0, 1));
        (await PostgresTestData.Scalar<string>(postgres.DataSource, """SELECT "Status" FROM "DiscoveredUrls" WHERE "Id" = @queued""", new { queued })).Should().Be("Idle");
        (await PostgresTestData.Scalar<string>(postgres.DataSource, """SELECT "Status" FROM "DiscoveredUrls" WHERE "Id" = @other""", new { other })).Should().Be("Idle");
    }

    [Fact]
    public async Task AStoppedUrlIsDisabledWithTheReasonUnselectedAndLeavesTheFailedCount()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var url = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/x/1");
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "CrawlJobs" SET "PathRules" = '[{"kind":"Exclude","prefix":"/x/"}]' WHERE "Id" = @job""", new { job });

        await new PostgresPathRuleApplier(postgres.DataSource, _time).Apply(job, default);

        var row = await PostgresTestData.Row(postgres.DataSource, """SELECT "Status", "FailReason" FROM "DiscoveredUrls" WHERE "Id" = @url""", new { url });
        ((string)row.Status, (string)row.FailReason).Should().Be(("Disabled", "unselected"));
        var progress = await new PostgresJobProgressReader(postgres.DataSource).Read(job, default);
        progress!.Failed.Should().Be(0);
    }
}
