using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// The links of a page: one row for each distinct target on each page. ToUrl is the normalized absolute target, the same
/// text as DiscoveredUrls.NormalizedUrl, at most 2048 characters. A btree on the raw text can exceed the index row limit,
/// so the unique key and the incoming lookup use md5("ToUrl"). The table is new and empty, so its indexes are built normally.
/// Pages parsed before this migration have no rows until they are parsed again.
/// </summary>
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

        // Leads with FromUrlId, so it also serves the outgoing list of a page and the delete before a re-parse.
        Execute.Sql($"""CREATE UNIQUE INDEX "UX_Links_FromUrlId_ToUrlMd5" ON "{Table}" ("FromUrlId", md5("ToUrl"))""");

        // Incoming links of a page, earliest first (foundOn and the "in" list).
        Execute.Sql($"""CREATE INDEX "IX_Links_ToUrlMd5_FirstSeenAt_Id" ON "{Table}" (md5("ToUrl"), "FirstSeenAt", "Id")""");
    }

    public override void Down() => Delete.Table(Table);
}
