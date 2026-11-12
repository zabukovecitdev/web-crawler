using FluentAssertions;

namespace SamoBot.Tests.Postgres;

[Collection(PostgresCollection.Name)]
[Trait("Category", "Integration")]
public class PostgresFixtureTests(PostgresFixture postgres)
{
    [Fact]
    public async Task Migrations_CreateTheRedesignedSchema()
    {
        await using var command = postgres.DataSource.CreateCommand(
            """SELECT count(*) FROM information_schema.tables WHERE table_name IN ('Hosts', 'DiscoveredUrls') AND table_schema = 'public'""");

        var count = (long)(await command.ExecuteScalarAsync())!;

        count.Should().Be(2);
    }
}
