using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000028, "Record whether a fetch stopped reading at the body size cap")]
public class AddBodyTruncatedToUrlFetches : Migration
{
    private const string Table = TableNames.Database.UrlFetches;
    private const int FormerBodyCapBytes = 5 * 1024 * 1024;

    public override void Up()
    {
        Alter.Table(Table).AddColumn("BodyTruncated").AsBoolean().NotNullable().WithDefaultValue(false);
        Execute.Sql($"""UPDATE "{Table}" SET "BodyTruncated" = true WHERE "ContentLength" = {FormerBodyCapBytes}""");
    }

    public override void Down() => Delete.Column("BodyTruncated").FromTable(Table);
}
