using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000016, "Create the sitemap tables and the sitemap work queue")]
public class CreateSitemapTables : Migration
{
    private const string FilesTable = TableNames.Database.SitemapFiles;
    private const string UrlsTable = TableNames.Database.SitemapUrls;
    private const string RequestsTable = TableNames.Database.SitemapRequests;

    public override void Up()
    {
        CreateFiles();
        CreateUrls();
        CreateRequests();
    }

    private void CreateFiles()
    {
        Create.Table(FilesTable)
            .WithColumn("Id").AsInt32().PrimaryKey().Identity()
            .WithColumn("Host").AsString(2048).NotNullable()
            .WithColumn("Url").AsString(2048).NotNullable()
            .WithColumn("ParentUrl").AsString(2048).Nullable()
            .WithColumn("Kind").AsString(20).NotNullable()
            .WithColumn("UrlCount").AsInt32().NotNullable().WithDefaultValue(0)
            .WithColumn("FetchedAt").AsDateTimeOffset().NotNullable();

        Create.Index("IX_SitemapFiles_Host").OnTable(FilesTable).OnColumn("Host");
    }

    private void CreateUrls()
    {
        // NormalizedLoc is the key to DiscoveredUrls.NormalizedUrl, as the crawler normalizes URLs.
        Create.Table(UrlsTable)
            .WithColumn("Id").AsInt64().PrimaryKey().Identity()
            .WithColumn("Host").AsString(2048).NotNullable()
            .WithColumn("Loc").AsString(2048).NotNullable()
            .WithColumn("NormalizedLoc").AsString(2048).NotNullable()
            .WithColumn("LastModified").AsDateTimeOffset().Nullable()
            .WithColumn("SitemapFileId").AsInt32().NotNullable();

        Create.ForeignKey("FK_SitemapUrls_SitemapFiles")
            .FromTable(UrlsTable).ForeignColumn("SitemapFileId")
            .ToTable(FilesTable).PrimaryColumn("Id")
            .OnDelete(System.Data.Rule.Cascade);

        // The page of URLs reads Host and orders by Loc. The crawl state joins on Host and NormalizedLoc.
        Create.UniqueConstraint("UQ_SitemapUrls_Host_Loc").OnTable(UrlsTable).Columns("Host", "Loc");
        Create.Index("IX_SitemapUrls_Host_NormalizedLoc").OnTable(UrlsTable).OnColumn("Host").Ascending().OnColumn("NormalizedLoc").Ascending();
        Create.Index("IX_SitemapUrls_SitemapFileId").OnTable(UrlsTable).OnColumn("SitemapFileId");

        // The crawl state and the not-in-sitemap count look up crawled pages by host and normalized URL.
        Create.Index("IX_DiscoveredUrls_Host_NormalizedUrl").OnTable(TableNames.Database.DiscoveredUrls)
            .OnColumn("Host").Ascending().OnColumn("NormalizedUrl").Ascending();
    }

    private void CreateRequests()
    {
        Create.Table(RequestsTable)
            .WithColumn("Host").AsString(2048).PrimaryKey()
            .WithColumn("RequestedAt").AsDateTimeOffset().NotNullable()
            .WithColumn("LeaseUntil").AsDateTimeOffset().Nullable()
            .WithColumn("CompletedAt").AsDateTimeOffset().Nullable();

        Create.Index("IX_SitemapRequests_CompletedAt_RequestedAt").OnTable(RequestsTable)
            .OnColumn("CompletedAt").Ascending().OnColumn("RequestedAt").Ascending();
    }

    public override void Down()
    {
        Delete.Table(RequestsTable);
        Delete.Index("IX_DiscoveredUrls_Host_NormalizedUrl").OnTable(TableNames.Database.DiscoveredUrls);
        Delete.Table(UrlsTable);
        Delete.Table(FilesTable);
    }
}
