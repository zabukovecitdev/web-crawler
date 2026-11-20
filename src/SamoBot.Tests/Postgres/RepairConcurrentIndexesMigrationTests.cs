using FluentAssertions;
using SamoBot.Migrations.Migrations;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public sealed class RepairConcurrentIndexesMigrationTests(PostgresFixture postgres) : IAsyncLifetime
{
    private const long Before = 20240101000029;
    private const long After = 20240101000030;

    private ScratchDatabase _scratch = null!;

    public async Task InitializeAsync()
    {
        _scratch = await ScratchDatabase.Create(postgres);
        _scratch.MigrateUp(Before);
    }

    public async Task DisposeAsync() => await _scratch.DisposeAsync();

    private async Task Run(string sql)
    {
        await using var command = _scratch.Data.CreateCommand(sql);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<(uint Oid, bool Valid, string Definition)> State(string name)
    {
        var rows = await PostgresTestData.Rows<(long Oid, bool Valid, string Definition)>(_scratch.Data,
            """
            SELECT i.indexrelid::bigint, i.indisvalid, pg_get_indexdef(i.indexrelid)
            FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid WHERE c.relname = @name
            """,
            new { name });
        var row = rows.Single();
        return ((uint)row.Oid, row.Valid, row.Definition);
    }

    [Fact]
    public async Task UpRebuildsADriftedAnInvalidAndAMissingIndex()
    {
        await Run("""DROP INDEX "IX_DiscoveredUrls_Host_LastCrawlAt" """);
        await Run("""CREATE INDEX "IX_DiscoveredUrls_Host_LastCrawlAt" ON "DiscoveredUrls" ("Host", "LastCrawlAt" DESC)""");
        await Run("""UPDATE pg_index SET indisvalid = false WHERE indexrelid = '"IX_UrlFetches_DiscoveredUrlId_FetchedAt"'::regclass""");
        await Run("""DROP INDEX "IX_DiscoveredUrls_Host_NormalizedUrl" """);

        _scratch.MigrateUp(After);

        foreach (var index in RepairConcurrentIndexes.Indexes)
        {
            var (_, valid, definition) = await State(index.Name);
            RepairConcurrentIndexes.IsHealthy(valid, definition, index).Should().BeTrue(index.Name);
        }
    }

    [Fact]
    public async Task UpLeavesHealthyIndexesAlone()
    {
        var before = await Task.WhenAll(RepairConcurrentIndexes.Indexes.Select(index => State(index.Name)));

        _scratch.MigrateUp(After);

        var after = await Task.WhenAll(RepairConcurrentIndexes.Indexes.Select(index => State(index.Name)));
        after.Select(state => state.Oid).Should().Equal(before.Select(state => state.Oid));
    }

    [Theory]
    [InlineData(20240101000014, "Migration 014")]
    [InlineData(20240101000005, "Migration 005")]
    public async Task LossyDownMigrationsRefuseAndSayWhy(long version, string migration)
    {
        await using var fresh = await ScratchDatabase.Create(postgres);
        fresh.MigrateUp(version);

        var down = () => fresh.MigrateDown(version - 1);

        down.Should().Throw<Exception>().Which.ToString().Should().Contain(migration).And.Contain("backup");
    }
}
