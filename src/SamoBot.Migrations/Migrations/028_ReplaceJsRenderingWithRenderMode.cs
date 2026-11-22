using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000028, "Replace the JavaScript switch of a crawl with a render mode, and add path rules and pause reasons")]
public class ReplaceJsRenderingWithRenderMode : Migration
{
    private const string Table = TableNames.Database.CrawlJobs;

    public override void Up()
    {
        Alter.Table(Table)
            .AddColumn("RenderMode").AsString(10).NotNullable().WithDefaultValue("Never")
            .AddColumn("JsNeeded").AsBoolean().NotNullable().WithDefaultValue(false)
            .AddColumn("SeedFromSitemap").AsBoolean().NotNullable().WithDefaultValue(false)
            .AddColumn("FollowLinkedPages").AsBoolean().NotNullable().WithDefaultValue(true)
            .AddColumn("PathRules").AsString().NotNullable().WithDefaultValue("[]")
            .AddColumn("ReviewAfterPages").AsInt32().Nullable()
            .AddColumn("PauseReason").AsString(50).Nullable()
            .AddColumn("PauseDetail").AsString(2048).Nullable();

        Execute.Sql($"""UPDATE "{Table}" SET "RenderMode" = 'Always' WHERE "UseJsRendering" """);
        Execute.Sql($"""ALTER TABLE "{Table}" ALTER COLUMN "RenderMode" SET DEFAULT 'Auto'""");
        Delete.Column("UseJsRendering").FromTable(Table);
    }

    public override void Down()
    {
        Alter.Table(Table).AddColumn("UseJsRendering").AsBoolean().NotNullable().WithDefaultValue(false);
        Execute.Sql($"""UPDATE "{Table}" SET "UseJsRendering" = true WHERE "RenderMode" = 'Always' OR ("RenderMode" = 'Auto' AND "JsNeeded")""");
        Delete.Column("RenderMode").FromTable(Table);
        Delete.Column("JsNeeded").FromTable(Table);
        Delete.Column("SeedFromSitemap").FromTable(Table);
        Delete.Column("FollowLinkedPages").FromTable(Table);
        Delete.Column("PathRules").FromTable(Table);
        Delete.Column("ReviewAfterPages").FromTable(Table);
        Delete.Column("PauseReason").FromTable(Table);
        Delete.Column("PauseDetail").FromTable(Table);
    }
}
