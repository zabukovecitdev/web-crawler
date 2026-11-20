using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

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
