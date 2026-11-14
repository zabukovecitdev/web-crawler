using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000018, "Mark every parsed document as not indexed so the index loop re-sends it with host, url and fetch time")]
public class ReindexForDashboardSearch : Migration
{
    public override void Up() =>
        Execute.Sql($@"UPDATE ""{TableNames.Database.ParsedDocuments}"" SET ""IndexedAt"" = NULL WHERE ""IndexedAt"" IS NOT NULL;");

    // Re-indexing is idempotent and has nothing to undo.
    public override void Down()
    {
    }
}
