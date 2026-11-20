using System.Text.Json;
using FluentAssertions;
using FluentMigrator.Runner;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using SamoBot.Migrations.Migrations;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public sealed class BackfillLegacyRowsMigrationTests(PostgresFixture postgres) : IAsyncLifetime
{
    private const long Before = 20240101000028;
    private const long After = 20240101000029;

    private readonly string _database = $"migration_{Guid.NewGuid():N}";
    private string _connectionString = string.Empty;
    private NpgsqlDataSource _data = null!;

    public async Task InitializeAsync()
    {
        await using (var create = postgres.DataSource.CreateCommand($"CREATE DATABASE \"{_database}\""))
        {
            await create.ExecuteNonQueryAsync();
        }

        _connectionString = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Database = _database }.ConnectionString;
        _data = NpgsqlDataSource.Create(_connectionString);
        Migrate(Before);
    }

    public async Task DisposeAsync()
    {
        await _data.DisposeAsync();
        await using (var pooled = new NpgsqlConnection(_connectionString))
        {
            NpgsqlConnection.ClearPool(pooled);
        }

        await using var drop = postgres.DataSource.CreateCommand($"DROP DATABASE \"{_database}\" WITH (FORCE)");
        await drop.ExecuteNonQueryAsync();
    }

    private void Migrate(long version)
    {
        using var provider = new ServiceCollection()
            .AddFluentMigratorCore()
            .ConfigureRunner(runner => runner.AddPostgres().WithGlobalConnectionString(_connectionString)
                .ScanIn(typeof(BackfillLegacyRows).Assembly).For.Migrations())
            .BuildServiceProvider();
        provider.GetRequiredService<IMigrationRunner>().MigrateUp(version);
    }

    private async Task<int> Document(int fetchId, string? policy, string? robots) =>
        await PostgresTestData.Scalar<int>(_data,
            """
            INSERT INTO "ParsedDocuments" ("UrlFetchId", "Policy", "RobotsDirectives", "IndexedAt")
            VALUES (@fetchId, @policy, @robots, now()) RETURNING "Id"
            """,
            new { fetchId, policy, robots });

    private async Task<int> NewUrl(int job, string url) =>
        await PostgresTestData.InsertFetch(_data, await PostgresTestData.InsertUrl(_data, job, url), Guid.NewGuid().ToString("N"));

    private Task<bool> Indexed(int document) =>
        PostgresTestData.Scalar<bool>(_data, """SELECT "IndexedAt" IS NOT NULL FROM "ParsedDocuments" WHERE "Id" = @document""", new { document });

    private async Task<int> Event(int jobId, string type, string payload) =>
        await PostgresTestData.Scalar<int>(_data,
            """INSERT INTO "CrawlJobEvents" ("CrawlJobId", "EventType", "Payload") VALUES (@jobId, @type, @payload) RETURNING "Id" """,
            new { jobId, type, payload });

    private async Task<JsonElement> Json(string sql, int id) =>
        JsonDocument.Parse(await PostgresTestData.Scalar<string>(_data, sql, new { id })).RootElement;

    [Fact]
    public async Task UpBuildsThePolicyFromTheOldRobotsFlags()
    {
        var job = await PostgresTestData.InsertJob(_data);
        var restricted = await Document(await NewUrl(job, "https://a.example/r"),
            null, """{"content":" noindex, nofollow ","noIndex":true,"noFollow":true,"noArchive":false,"noSnippet":true,"none":false}""");
        var none = await Document(await NewUrl(job, "https://a.example/n"), null, """{"Content":"none","None":true}""");

        Migrate(After);

        const string policy = """SELECT "Policy" FROM "ParsedDocuments" WHERE "Id" = @id""";
        var built = await Json(policy, restricted);
        (built.GetProperty("index").GetBoolean(), built.GetProperty("followLinks").GetBoolean(), built.GetProperty("archive").GetBoolean())
            .Should().Be((false, false, true));
        built.GetProperty("snippetMaxLength").GetInt32().Should().Be(0);
        built.GetProperty("directives").EnumerateArray().Single().GetString().Should().Be("noindex, nofollow");
        var fromNone = await Json(policy, none);
        (fromNone.GetProperty("index").GetBoolean(), fromNone.GetProperty("followLinks").GetBoolean()).Should().Be((false, false));
        (await Indexed(restricted)).Should().BeFalse("the index loop must remove a page that now says noindex");
    }

    [Fact]
    public async Task UpRepairsMissingAndUnreadablePoliciesAndKeepsGoodOnes()
    {
        var job = await PostgresTestData.InsertJob(_data);
        var plain = await Document(await NewUrl(job, "https://a.example/p"), null, null);
        var brokenRobots = await Document(await NewUrl(job, "https://a.example/b"), null, "not json");
        var nullLiteral = await Document(await NewUrl(job, "https://a.example/l"), "null", null);
        var truncated = await Document(await NewUrl(job, "https://a.example/t"), """{"index":fal""", null);
        var current = await Document(await NewUrl(job, "https://a.example/c"), """{"index":false,"directives":["noindex"]}""", null);

        Migrate(After);

        const string policy = """SELECT "Policy" FROM "ParsedDocuments" WHERE "Id" = @id""";
        foreach (var repaired in new[] { plain, brokenRobots, nullLiteral, truncated })
        {
            var json = await Json(policy, repaired);
            (json.GetProperty("index").GetBoolean(), json.GetProperty("directives").GetArrayLength()).Should().Be((true, 0));
            (await Indexed(repaired)).Should().BeTrue("an unrestricted default changes nothing in the index");
        }

        (await Json(policy, current)).GetProperty("index").GetBoolean().Should().BeFalse();
        var nullable = await PostgresTestData.Scalar<string>(_data,
            """SELECT is_nullable FROM information_schema.columns WHERE table_name = 'ParsedDocuments' AND column_name = 'Policy'""");
        nullable.Should().Be("NO");
    }

    [Fact]
    public async Task UpReindexesOnlyTheNewestDocumentOfAUrl()
    {
        var job = await PostgresTestData.InsertJob(_data);
        var url = await PostgresTestData.InsertUrl(_data, job, "https://a.example/x");
        var older = await Document(await PostgresTestData.InsertFetch(_data, url, "old"), null, """{"noIndex":true}""");
        var newer = await Document(await PostgresTestData.InsertFetch(_data, url, "new"), null, """{"noIndex":true}""");

        Migrate(After);

        (await Indexed(older)).Should().BeTrue();
        (await Indexed(newer)).Should().BeFalse();
    }

    [Fact]
    public async Task UpAddsThePageToOldFetchEventsAndDropsTheOnesItCannotComplete()
    {
        var job = await PostgresTestData.InsertJob(_data);
        var url = await PostgresTestData.InsertUrl(_data, job, "https://a.example/x");
        await PostgresTestData.Scalar<int>(_data,
            """
            INSERT INTO "DiscoveredUrls" ("Host", "Url", "NormalizedUrl", "Status", "NextCrawlAt", "CrawlJobId", "Depth")
            VALUES ('a.example', 'https://a.example/x', 'https://a.example/x?v2', 'Idle', now(), @job, 0) RETURNING "Id"
            """,
            new { job });
        var old = await Event(job, "FetchCompleted", """{"url":"https://a.example/x","statusCode":200,"contentStatus":"Stored"}""");
        var orphan = await Event(job, "FetchCompleted", """{"url":"https://a.example/gone","contentStatus":"Stored"}""");
        var preTelemetry = await Event(job, "FetchCompleted", """{"url":"https://a.example/x","statusCode":200,"outcome":"Success"}""");
        var started = await Event(job, "FetchStarted", """{"url":"https://a.example/gone"}""");
        var recent = await Event(job, "FetchCompleted", """{"pageId":99,"host":"b.example","url":"https://a.example/x","contentStatus":"Stored"}""");

        Migrate(After);

        const string payload = """SELECT "Payload" FROM "CrawlJobEvents" WHERE "Id" = @id""";
        var filled = await Json(payload, old);
        (filled.GetProperty("pageId").GetInt32(), filled.GetProperty("host").GetString(), filled.GetProperty("statusCode").GetInt32())
            .Should().Be((url, "a.example", 200), "the first of two rows with the same raw URL wins");
        (await Json(payload, recent)).GetProperty("pageId").GetInt32().Should().Be(99);
        var left = await PostgresTestData.Rows<int>(_data, """SELECT "Id" FROM "CrawlJobEvents" ORDER BY "Id" """);
        left.Should().Equal(old, started, recent).And.NotContain([orphan, preTelemetry]);
    }

    [Fact]
    public void DownRefusesBecauseDeletedEventsCannotBeRebuilt()
    {
        Migrate(After);

        var down = () =>
        {
            using var provider = new ServiceCollection()
                .AddFluentMigratorCore()
                .ConfigureRunner(runner => runner.AddPostgres().WithGlobalConnectionString(_connectionString)
                    .ScanIn(typeof(BackfillLegacyRows).Assembly).For.Migrations())
                .BuildServiceProvider();
            provider.GetRequiredService<IMigrationRunner>().MigrateDown(Before);
        };

        down.Should().Throw<Exception>().Which.ToString().Should().Contain("backup");
    }
}
