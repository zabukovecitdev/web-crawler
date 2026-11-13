using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000015, "Add indexes for the dashboard read models")]
public class AddDashboardIndexes : Migration
{
    private const string FetchHistoryIndex = "IX_UrlFetches_DiscoveredUrlId_FetchedAt";
    private const string HostPagesIndex = "IX_DiscoveredUrls_Host_LastCrawlAt";

    public override void Up()
    {
        // The page detail and content queries read the newest fetches of one URL in FetchedAt order.
        Create.Index(FetchHistoryIndex)
            .OnTable(TableNames.Database.UrlFetches)
            .OnColumn("DiscoveredUrlId").Ascending()
            .OnColumn("FetchedAt").Descending();

        // The host page list filters on Host and orders by LastCrawlAt, newest first.
        Create.Index(HostPagesIndex)
            .OnTable(TableNames.Database.DiscoveredUrls)
            .OnColumn("Host").Ascending()
            .OnColumn("LastCrawlAt").Descending();
    }

    public override void Down()
    {
        Delete.Index(HostPagesIndex).OnTable(TableNames.Database.DiscoveredUrls);
        Delete.Index(FetchHistoryIndex).OnTable(TableNames.Database.UrlFetches);
    }
}
