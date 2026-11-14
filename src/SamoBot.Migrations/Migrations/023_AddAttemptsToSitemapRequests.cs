using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000023, "Count the claims of a sitemap request, so a host that keeps failing stops being retried")]
public class AddAttemptsToSitemapRequests : Migration
{
    private const string Table = TableNames.Database.SitemapRequests;

    public override void Up() =>
        Alter.Table(Table).AddColumn("Attempts").AsInt32().NotNullable().WithDefaultValue(0);

    public override void Down() => Delete.Column("Attempts").FromTable(Table);
}
