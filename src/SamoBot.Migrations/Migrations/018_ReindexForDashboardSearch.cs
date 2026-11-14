using FluentMigrator;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000018, "Superseded by 021: no change")]
public class ReindexForDashboardSearch : Migration
{
    // Superseded by 021. The full-table UPDATE that was here is replaced by a narrowed one that re-sends one document per URL.
    public override void Up()
    {
    }

    public override void Down()
    {
    }
}
