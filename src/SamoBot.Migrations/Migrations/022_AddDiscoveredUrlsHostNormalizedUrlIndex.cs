using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000022, TransactionBehavior.None, "Add the DiscoveredUrls (Host, NormalizedUrl) index concurrently")]
public class AddDiscoveredUrlsHostNormalizedUrlIndex : Migration
{
    private const string IndexName = "IX_DiscoveredUrls_Host_NormalizedUrl";

    public override void Up() => Execute.Sql(
        $"""CREATE INDEX CONCURRENTLY IF NOT EXISTS "{IndexName}" ON "{TableNames.Database.DiscoveredUrls}" ("Host", "NormalizedUrl")""");

    public override void Down() => Execute.Sql($"""DROP INDEX CONCURRENTLY IF EXISTS "{IndexName}" """);
}
