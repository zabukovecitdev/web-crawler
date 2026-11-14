using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// The crawl state and the not-in-sitemap count look up crawled pages by host and normalized URL. The index is built with
/// CONCURRENTLY so that crawling is not blocked on a large table, so this migration runs outside a transaction and each
/// Execute.Sql holds one statement. A database that ran the earlier 016 already has the index, and IF NOT EXISTS skips it.
/// Known limit: a failed concurrent build leaves an INVALID index that IF NOT EXISTS then skips. Fix it with
/// DROP INDEX CONCURRENTLY "IX_DiscoveredUrls_Host_NormalizedUrl" and run the migrations again.
/// </summary>
[Migration(20240101000022, TransactionBehavior.None, "Add the DiscoveredUrls (Host, NormalizedUrl) index concurrently")]
public class AddDiscoveredUrlsHostNormalizedUrlIndex : Migration
{
    private const string IndexName = "IX_DiscoveredUrls_Host_NormalizedUrl";

    public override void Up() => Execute.Sql(
        $"""CREATE INDEX CONCURRENTLY IF NOT EXISTS "{IndexName}" ON "{TableNames.Database.DiscoveredUrls}" ("Host", "NormalizedUrl")""");

    public override void Down() => Execute.Sql($"""DROP INDEX CONCURRENTLY IF EXISTS "{IndexName}" """);
}
