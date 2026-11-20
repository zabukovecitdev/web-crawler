using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000025, "Create the Links table")]
public class CreateLinksTable : Migration
{
    private const string Table = TableNames.Database.Links;

    public override void Up()
    {
        Create.Table(Table)
            .WithColumn("Id").AsInt64().PrimaryKey().Identity()
            .WithColumn("FromUrlId").AsInt32().NotNullable()
            .WithColumn("ToUrl").AsString(2048).NotNullable()
            .WithColumn("ToHost").AsString(2048).NotNullable()
            .WithColumn("AnchorText").AsString(200).Nullable()
            .WithColumn("IsInternal").AsBoolean().NotNullable()
            .WithColumn("NoFollow").AsBoolean().NotNullable()
            .WithColumn("FirstSeenAt").AsDateTimeOffset().NotNullable();

        Create.ForeignKey("FK_Links_DiscoveredUrls")
            .FromTable(Table).ForeignColumn("FromUrlId")
            .ToTable(TableNames.Database.DiscoveredUrls).PrimaryColumn("Id")
            .OnDelete(System.Data.Rule.Cascade);

        Execute.Sql($"""CREATE UNIQUE INDEX "UX_Links_FromUrlId_ToUrlMd5" ON "{Table}" ("FromUrlId", md5("ToUrl"))""");

        Execute.Sql($"""CREATE INDEX "IX_Links_ToUrlMd5_FirstSeenAt_Id" ON "{Table}" (md5("ToUrl"), "FirstSeenAt", "Id")""");
    }

    public override void Down() => Delete.Table(Table);
}
