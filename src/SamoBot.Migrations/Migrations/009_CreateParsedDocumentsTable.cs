using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000009, "Create ParsedDocuments table")]
public class CreateParsedDocumentsTable : Migration
{
    public override void Up()
    {
        Create.Table(TableNames.Database.ParsedDocuments)
            .WithColumn("Id").AsInt32().PrimaryKey().Identity()
            .WithColumn("UrlFetchId").AsInt32().NotNullable().Unique()
            .WithColumn("Title").AsString(500).Nullable()
            .WithColumn("Description").AsString(2000).Nullable()
            .WithColumn("Keywords").AsString(1000).Nullable()
            .WithColumn("Author").AsString(255).Nullable()
            .WithColumn("Language").AsString(10).Nullable()
            .WithColumn("Canonical").AsString(2048).Nullable()
            .WithColumn("BodyText").AsString().Nullable()
            .WithColumn("Headings").AsString().Nullable()
            .WithColumn("Images").AsString().Nullable()
            .WithColumn("RobotsDirectives").AsString().Nullable()
            .WithColumn("OpenGraphData").AsString().Nullable()
            .WithColumn("TwitterCardData").AsString().Nullable()
            .WithColumn("JsonLdData").AsString().Nullable()
            .WithColumn("ParsedAt").AsDateTimeOffset().NotNullable().WithDefault(SystemMethods.CurrentDateTime);

        Create.ForeignKey("FK_ParsedDocuments_UrlFetches")
            .FromTable(TableNames.Database.ParsedDocuments)
            .ForeignColumn("UrlFetchId")
            .ToTable(TableNames.Database.UrlFetches)
            .PrimaryColumn("Id")
            .OnDelete(System.Data.Rule.Cascade);

        Create.Index("IX_ParsedDocuments_ParsedAt")
            .OnTable(TableNames.Database.ParsedDocuments)
            .OnColumn("ParsedAt");
    }

    public override void Down()
    {
        Delete.Table(TableNames.Database.ParsedDocuments);
    }
}
