using FluentAssertions;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class MigrationIndexTests(PostgresFixture postgres)
{
    private const string HostPagesIndex = "IX_DiscoveredUrls_Host_LastCrawlAt";
    private const string FetchHistoryIndex = "IX_UrlFetches_DiscoveredUrlId_FetchedAt";

    [Fact]
    public async Task TheHostPagesIndexMatchesTheOrderByOfTheHostPageList()
    {
        var definition = await PostgresTestData.Scalar<string>(
            postgres.DataSource,
            "SELECT indexdef FROM pg_indexes WHERE indexname = @name",
            new { name = HostPagesIndex });

        definition.Should().Contain("\"LastCrawlAt\" DESC NULLS LAST");
        definition.Should().Contain("\"Id\"");
    }

    [Theory]
    [InlineData(HostPagesIndex)]
    [InlineData(FetchHistoryIndex)]
    public async Task TheConcurrentlyBuiltIndexesAreValid(string name)
    {
        var valid = await PostgresTestData.Scalar<bool>(
            postgres.DataSource,
            """
            SELECT i.indisvalid
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indexrelid
            WHERE c.relname = @name
            """,
            new { name });

        valid.Should().BeTrue();
    }
}
