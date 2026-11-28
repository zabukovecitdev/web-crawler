using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000005, "Add LastFetchId to DiscoveredUrls and remove old fetch metadata columns")]
public class AddLastFetchIdToDiscoveredUrlsAndRemoveOldColumns : Migration
{
    public override void Up()
    {
        Alter.Table(TableNames.Database.DiscoveredUrls)
            .AddColumn("LastFetchId").AsInt32().Nullable();

        Create.ForeignKey("FK_DiscoveredUrls_UrlFetches")
            .FromTable(TableNames.Database.DiscoveredUrls)
            .ForeignColumn("LastFetchId")
            .ToTable(TableNames.Database.UrlFetches)
            .PrimaryColumn("Id")
            .OnDelete(System.Data.Rule.SetNull);

        Create.Index("IX_DiscoveredUrls_LastFetchId")
            .OnTable(TableNames.Database.DiscoveredUrls)
            .OnColumn("LastFetchId");

        Delete.Column("ContentType")
            .Column("ContentLength")
            .Column("ObjectName")
            .Column("LastStatusCode")
            .FromTable(TableNames.Database.DiscoveredUrls);
    }

    public override void Down() =>
        throw new NotSupportedException(
            "Migration 005 dropped the fetch columns of DiscoveredUrls with their data. Restore a database backup taken before it to go back.");
}
