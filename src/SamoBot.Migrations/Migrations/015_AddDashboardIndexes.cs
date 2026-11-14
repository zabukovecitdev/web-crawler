using FluentMigrator;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// The two indexes are built CONCURRENTLY so a large crawl database keeps accepting writes. That needs no surrounding
/// transaction and exactly one statement per Execute.Sql. If a concurrent build fails it leaves an INVALID index that
/// IF NOT EXISTS then skips: run DROP INDEX CONCURRENTLY on it and migrate again.
/// A development database that ran the earlier (non-concurrent) version of this migration keeps its old
/// IX_DiscoveredUrls_Host_LastCrawlAt definition, which is correct but slower for the host page sort.
/// </summary>
[Migration(20240101000015, TransactionBehavior.None, "Add indexes for the dashboard read models")]
public class AddDashboardIndexes : Migration
{
    private const string FetchHistoryIndex = "IX_UrlFetches_DiscoveredUrlId_FetchedAt";
    private const string HostPagesIndex = "IX_DiscoveredUrls_Host_LastCrawlAt";

    public override void Up()
    {
        // The page detail and content queries read the newest fetches of one URL in FetchedAt order.
        Execute.Sql($"""CREATE INDEX CONCURRENTLY IF NOT EXISTS "{FetchHistoryIndex}" ON "UrlFetches" ("DiscoveredUrlId", "FetchedAt" DESC)""");

        // The host page list filters on Host and orders by LastCrawlAt DESC NULLS LAST, then Id.
        Execute.Sql($"""CREATE INDEX CONCURRENTLY IF NOT EXISTS "{HostPagesIndex}" ON "DiscoveredUrls" ("Host", "LastCrawlAt" DESC NULLS LAST, "Id")""");
    }

    public override void Down()
    {
        Execute.Sql($"""DROP INDEX CONCURRENTLY IF EXISTS "{HostPagesIndex}" """);
        Execute.Sql($"""DROP INDEX CONCURRENTLY IF EXISTS "{FetchHistoryIndex}" """);
    }
}
