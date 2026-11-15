using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// robots.txt is always respected, so the switch that turned it off is gone. Jobs and URLs that were created with the
/// switch off are checked against robots.txt from now on.
/// </summary>
[Migration(20240101000027, "Remove the switch that turned robots.txt off")]
public class DropRespectRobots : Migration
{
    private const string Column = "RespectRobots";

    public override void Up()
    {
        Delete.Column(Column).FromTable(TableNames.Database.DiscoveredUrls);
        Delete.Column(Column).FromTable(TableNames.Database.CrawlJobs);
    }

    public override void Down()
    {
        Alter.Table(TableNames.Database.CrawlJobs).AddColumn(Column).AsBoolean().NotNullable().WithDefaultValue(true);
        Alter.Table(TableNames.Database.DiscoveredUrls).AddColumn(Column).AsBoolean().NotNullable().WithDefaultValue(true);
    }
}
