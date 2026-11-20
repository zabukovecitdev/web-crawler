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
        NpgsqlConnection.ClearAllPools();
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
            INSERT INTO "ParsedDocuments" ("UrlFetchId", "Policy", "RobotsDirectives") VALUES (@fetchId, @policy, @robots) RETURNING "Id"
            """,
            new { fetchId, policy, robots });

    private async Task<int> Event(int jobId, string type, string payload) =>
        await PostgresTestData.Scalar<int>(_data,
            """INSERT INTO "CrawlJobEvents" ("CrawlJobId", "EventType", "Payload") VALUES (@jobId, @type, @payload) RETURNING "Id" """,
            new { jobId, type, payload });

    private async Task<JsonElement> Json(string sql, int id) =>
        JsonDocument.Parse(await PostgresTestData.Scalar<string>(_data, sql, new { id })).RootElement;

    [Fact]
    public async Task UpGivesEveryDocumentAPolicyAndKeepsTheOldRobotsText()
    {
        var job = await PostgresTestData.InsertJob(_data);
        var url = await PostgresTestData.InsertUrl(_data, job, "https://a.example/x");
        var withRobots = await Document(await PostgresTestData.InsertFetch(_data, url, "a"), null, """{"content":" noindex, nofollow "}""");
        var plain = await Document(await PostgresTestData.InsertFetch(_data, url, "b"), null, null);
        var broken = await Document(await PostgresTestData.InsertFetch(_data, url, "c"), null, "not json");
        var current = await Document(await PostgresTestData.InsertFetch(_data, url, "d"), """{"index":false,"directives":["noindex"]}""", null);

        Migrate(After);

        const string policy = """SELECT "Policy" FROM "ParsedDocuments" WHERE "Id" = @id""";
        (await Json(policy, withRobots)).GetProperty("directives").EnumerateArray().Single().GetString().Should().Be("noindex, nofollow");
        (await Json(policy, withRobots)).GetProperty("index").GetBoolean().Should().BeTrue();
        (await Json(policy, plain)).GetProperty("directives").GetArrayLength().Should().Be(0);
        (await Json(policy, broken)).GetProperty("directives").GetArrayLength().Should().Be(0);
        (await Json(policy, current)).GetProperty("index").GetBoolean().Should().BeFalse();
        var columns = await PostgresTestData.Rows<string>(_data,
            """SELECT column_name FROM information_schema.columns WHERE table_name = 'ParsedDocuments' AND column_name IN ('RobotsDirectives', 'Policy') AND is_nullable = 'NO'""");
        columns.Should().Equal("Policy");
    }

    [Fact]
    public async Task UpAddsThePageToOldFetchEventsAndDropsTheOnesWithoutAPage()
    {
        var job = await PostgresTestData.InsertJob(_data);
        var url = await PostgresTestData.InsertUrl(_data, job, "https://a.example/x");
        var old = await Event(job, "FetchCompleted", """{"url":"https://a.example/x","statusCode":200}""");
        var orphan = await Event(job, "FetchCompleted", """{"url":"https://a.example/gone"}""");
        var started = await Event(job, "FetchStarted", """{"url":"https://a.example/gone"}""");
        var recent = await Event(job, "FetchCompleted", """{"pageId":99,"host":"b.example","url":"https://a.example/x"}""");

        Migrate(After);

        const string payload = """SELECT "Payload" FROM "CrawlJobEvents" WHERE "Id" = @id""";
        var filled = await Json(payload, old);
        (filled.GetProperty("pageId").GetInt32(), filled.GetProperty("host").GetString(), filled.GetProperty("statusCode").GetInt32())
            .Should().Be((url, "a.example", 200));
        (await Json(payload, recent)).GetProperty("pageId").GetInt32().Should().Be(99);
        var left = await PostgresTestData.Rows<int>(_data, """SELECT "Id" FROM "CrawlJobEvents" ORDER BY "Id" """);
        left.Should().Equal(old, started, recent).And.NotContain(orphan);
    }
}
