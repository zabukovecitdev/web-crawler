using FluentMigrator;

namespace SamoBot.Migrations.Migrations;

/// <summary>
/// Search documents are keyed by the URL id and live in the Meilisearch index "pages". This marks the newest parsed document
/// of every URL as not indexed so the index loop fills the new index with one document per URL. Older documents of a URL
/// are left alone. The previous index "parsed_documents" is no longer used and can be deleted with
/// curl -X DELETE -H "Authorization: Bearer $MEILI_MASTER_KEY" http://localhost:7700/indexes/parsed_documents
/// </summary>
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

    // Re-indexing is idempotent and has nothing to undo.
    public override void Down()
    {
    }
}
