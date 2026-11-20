using FluentMigrator;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000021, "Re-send the newest parsed document of every URL to the pages search index")]
public class ReindexLatestDocuments : Migration
{
    public const string Statement = """
        WITH latest AS (
            SELECT DISTINCT ON (f."DiscoveredUrlId") d."Id"
            FROM "ParsedDocuments" d JOIN "UrlFetches" f ON f."Id" = d."UrlFetchId"
            ORDER BY f."DiscoveredUrlId", d."Id" DESC)
        UPDATE "ParsedDocuments" SET "IndexedAt" = NULL
        WHERE "Id" IN (SELECT "Id" FROM latest) AND "IndexedAt" IS NOT NULL
        """;

    public override void Up() => Execute.Sql(Statement);

    public override void Down()
    {
    }
}
