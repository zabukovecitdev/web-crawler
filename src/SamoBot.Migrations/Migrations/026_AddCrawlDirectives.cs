using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// The crawl directives. UrlFetches.ResponseHeaders: the response headers the directive sources read (X-Robots-Tag, Link),
/// as a JSON object of name to values; NULL when none were sent. ParsedDocuments.Policy: what the crawler may do with the
/// page (index, follow, snippet, archive), as JSON; NULL for pages parsed before, which read as the default policy.
/// The newest document of every URL is re-sent to the search index, which now carries the fields the policy needs.
/// </summary>
[Migration(20240101000026, "Store response headers and page policies for the crawl directives")]
public class AddCrawlDirectives : Migration
{
    public override void Up()
    {
        Alter.Table(TableNames.Database.UrlFetches).AddColumn("ResponseHeaders").AsString().Nullable();
        Alter.Table(TableNames.Database.ParsedDocuments).AddColumn("Policy").AsString().Nullable();
        Execute.Sql(ReindexLatestDocuments.Statement);
    }

    public override void Down()
    {
        Delete.Column("ResponseHeaders").FromTable(TableNames.Database.UrlFetches);
        Delete.Column("Policy").FromTable(TableNames.Database.ParsedDocuments);
    }
}
