using FluentAssertions;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresDatabaseDefinition.Name)]
[Trait("Category", "Integration")]
public class PostgresFixtureTests(PostgresFixture postgres)
{
    [Fact]
    public async Task MigrationsCreateTheRedesignedSchema()
    {
        await using var command = postgres.DataSource.CreateCommand(
            """SELECT count(*) FROM information_schema.tables WHERE table_name IN ('Hosts', 'DiscoveredUrls') AND table_schema = 'public'""");

        var count = (long)(await command.ExecuteScalarAsync())!;

        count.Should().Be(2);
    }
}
