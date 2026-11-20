using FluentMigrator;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000015, TransactionBehavior.None, "Add indexes for the dashboard read models")]
public class AddDashboardIndexes : Migration
{
    private const string FetchHistoryIndex = "IX_UrlFetches_DiscoveredUrlId_FetchedAt";
    private const string HostPagesIndex = "IX_DiscoveredUrls_Host_LastCrawlAt";

    public override void Up()
    {
        Execute.Sql($"""CREATE INDEX CONCURRENTLY IF NOT EXISTS "{FetchHistoryIndex}" ON "UrlFetches" ("DiscoveredUrlId", "FetchedAt" DESC)""");

        Execute.Sql($"""CREATE INDEX CONCURRENTLY IF NOT EXISTS "{HostPagesIndex}" ON "DiscoveredUrls" ("Host", "LastCrawlAt" DESC NULLS LAST, "Id")""");
    }

    public override void Down()
    {
        Execute.Sql($"""DROP INDEX CONCURRENTLY IF EXISTS "{HostPagesIndex}" """);
        Execute.Sql($"""DROP INDEX CONCURRENTLY IF EXISTS "{FetchHistoryIndex}" """);
    }
}
