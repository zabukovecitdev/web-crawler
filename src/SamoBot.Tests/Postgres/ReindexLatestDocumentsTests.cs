using Dapper;
using FluentAssertions;
using SamoBot.Migrations.Migrations;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class ReindexLatestDocumentsTests(PostgresFixture postgres) : IAsyncLifetime
{
    public Task InitializeAsync() => postgres.Reset();

    public Task DisposeAsync() => Task.CompletedTask;

    private async Task<int> Parsed(int urlId)
    {
        var fetchId = await PostgresTestData.InsertFetch(postgres.DataSource, urlId);
        return await PostgresTestData.Scalar<int>(
            postgres.DataSource,
            """
            INSERT INTO "ParsedDocuments" ("UrlFetchId", "Title", "IndexedAt", "Policy") VALUES (@fetchId, 't', now(), '{}') RETURNING "Id"
            """,
            new { fetchId });
    }

    private Task<bool> Pending(int id) =>
        PostgresTestData.Scalar<bool>(postgres.DataSource, """SELECT "IndexedAt" IS NULL FROM "ParsedDocuments" WHERE "Id" = @id""", new { id });

    [Fact]
    public async Task OnlyTheNewestDocumentOfEachUrlIsMarkedNotIndexed()
    {
        var job = await PostgresTestData.InsertJob(postgres.DataSource);
        var first = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/1");
        var second = await PostgresTestData.InsertUrl(postgres.DataSource, job, "https://a.example/2");
        var oldOne = await Parsed(first);
        var newOne = await Parsed(first);
        var single = await Parsed(second);

        await using (var connection = await postgres.DataSource.OpenConnectionAsync())
        {
            await connection.ExecuteAsync(ReindexLatestDocuments.Statement);
        }

        (await Pending(oldOne)).Should().BeFalse();
        (await Pending(newOne)).Should().BeTrue();
        (await Pending(single)).Should().BeTrue();
    }
}
