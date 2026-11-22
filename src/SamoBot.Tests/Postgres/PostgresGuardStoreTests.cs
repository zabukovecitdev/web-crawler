using FluentAssertions;
using SamoBot.Adapters.Postgres;
using SamoBot.Core.Domain;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresGuardStoreTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private async Task<int> Job(string status, string mode, bool jsNeeded = false, int? review = null)
    {
        var id = await PostgresTestData.InsertJob(postgres.DataSource, status);
        await PostgresTestData.Execute(
            postgres.DataSource,
            """UPDATE "CrawlJobs" SET "RenderMode" = @mode, "JsNeeded" = @jsNeeded, "ReviewAfterPages" = @review WHERE "Id" = @id""",
            new { id, mode, jsNeeded, review });
        return id;
    }

    private async Task<(string Status, string? Reason)> State(int id)
    {
        var row = await PostgresTestData.Row(postgres.DataSource, """SELECT "Status", "PauseReason" FROM "CrawlJobs" WHERE "Id" = @id""", new { id });
        return ((string)row.Status, (string?)row.PauseReason);
    }

    [Fact]
    public async Task OnlyRunningJobsThatNeedTheBrowserArePausedForIt()
    {
        var always = await Job("Running", "Always");
        var autoJs = await Job("Running", "Auto", jsNeeded: true);
        var autoPlain = await Job("Running", "Auto");
        var never = await Job("Running", "Never", jsNeeded: true);
        var alreadyPaused = await Job("Paused", "Always");

        var paused = await new PostgresBrowserPauseStore(postgres.DataSource).PauseJobsNeedingBrowser(TestData.Now, default);

        paused.Should().BeEquivalentTo([always, autoJs]);
        (await State(always)).Should().Be(("Paused", PauseReasons.BrowserUnavailable));
        (await State(autoPlain)).Should().Be(("Running", null));
        (await State(never)).Should().Be(("Running", null));
        (await State(alreadyPaused)).Should().Be(("Paused", null));
    }

    [Fact]
    public async Task OnlyJobsPausedForTheBrowserGoOnWhenItIsBack()
    {
        var waiting = await Job("Running", "Always");
        var byUser = await Job("Paused", "Always");
        var store = new PostgresBrowserPauseStore(postgres.DataSource);
        await store.PauseJobsNeedingBrowser(TestData.Now, default);

        var resumed = await store.ResumeJobsPausedForBrowser(TestData.Now, default);

        resumed.Should().Equal(waiting);
        (await State(waiting)).Should().Be(("Running", null));
        (await State(byUser)).Should().Be(("Paused", null));
    }

    [Fact]
    public async Task AJobPausesAtTheReviewPointOnceEnoughPagesAreFetched()
    {
        var job = await Job("Running", "Never", review: 2);
        var other = await Job("Running", "Never");
        for (var i = 0; i < 3; i++)
        {
            var url = await PostgresTestData.InsertUrl(postgres.DataSource, job, $"https://a.example/{i}");
            var fetch = await PostgresTestData.InsertFetch(postgres.DataSource, url, parsed: true);
            await PostgresTestData.Execute(postgres.DataSource, """UPDATE "DiscoveredUrls" SET "LastFetchId" = @fetch WHERE "Id" = @url AND @i < 1""", new { fetch, url, i });
        }

        var store = new PostgresReviewPauseStore(postgres.DataSource);
        (await store.PauseJobsAtTheReviewPoint(TestData.Now, default)).Should().BeEmpty();
        await PostgresTestData.Execute(postgres.DataSource, """UPDATE "DiscoveredUrls" u SET "LastFetchId" = f."Id" FROM "UrlFetches" f WHERE f."DiscoveredUrlId" = u."Id" """);

        (await store.PauseJobsAtTheReviewPoint(TestData.Now, default)).Should().Equal(job);
        (await State(job)).Should().Be(("Paused", PauseReasons.Review));
        (await State(other)).Should().Be(("Running", null));
    }

    [Fact]
    public async Task TheSitemapPlanStatusCountsFilesUrlsAndTheRequest()
    {
        await PostgresTestData.Execute(
            postgres.DataSource,
            """
            INSERT INTO "SitemapFiles" ("Host", "Url", "Kind", "UrlCount", "FetchedAt") VALUES ('a.example', 'https://a.example/sitemap.xml', 'UrlSet', 2, now());
            INSERT INTO "SitemapUrls" ("Host", "Loc", "NormalizedLoc", "SitemapFileId")
                SELECT 'a.example', 'https://a.example/' || g, 'https://a.example/' || g, (SELECT max("Id") FROM "SitemapFiles") FROM generate_series(1, 2) g;
            INSERT INTO "SitemapRequests" ("Host", "RequestedAt", "CompletedAt", "Attempts") VALUES ('a.example', now(), now(), 0)
            """);
        var reader = new PostgresSitemapPlanReader(postgres.DataSource);

        var known = await reader.Status("a.example", default);
        var unknown = await reader.Status("b.example", default);

        (known.Files, known.Urls, known.CompletedAt is not null, known.Attempts).Should().Be((1, 2, true, 0));
        (unknown.Files, unknown.Urls, unknown.RequestedAt, unknown.CompletedAt).Should().Be((0, 0, null, null));
    }
}
