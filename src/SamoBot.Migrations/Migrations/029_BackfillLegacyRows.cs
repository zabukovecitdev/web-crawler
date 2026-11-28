using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000029, "Backfill page policies and event page ids so readers handle one shape")]
public class BackfillLegacyRows : Migration
{
    private const string Documents = TableNames.Database.ParsedDocuments;
    private const string Fetches = TableNames.Database.UrlFetches;
    private const string Events = TableNames.Database.CrawlJobEvents;
    private const string Urls = TableNames.Database.DiscoveredUrls;

    public const string BackfillPoliciesSql = $$"""
        WITH latest AS (
            SELECT DISTINCT ON (f."DiscoveredUrlId") d."Id"
            FROM "{{Documents}}" d JOIN "{{Fetches}}" f ON f."Id" = d."UrlFetchId"
            ORDER BY f."DiscoveredUrlId", d."Id" DESC),
        broken AS (
            SELECT d."Id", d."IndexedAt",
                   CASE WHEN d."RobotsDirectives" IS NULL THEN NULL
                        WHEN pg_input_is_valid(d."RobotsDirectives", 'jsonb') THEN d."RobotsDirectives"::jsonb END AS r
            FROM "{{Documents}}" d
            WHERE CASE WHEN d."Policy" IS NULL THEN true
                       WHEN pg_input_is_valid(d."Policy", 'jsonb') THEN jsonb_typeof(d."Policy"::jsonb) <> 'object'
                       ELSE true END),
        flags AS (
            SELECT b."Id", b."IndexedAt",
                   (coalesce(b.r -> 'noIndex', b.r -> 'NoIndex') = 'true'::jsonb) IS TRUE AS no_index,
                   (coalesce(b.r -> 'noFollow', b.r -> 'NoFollow') = 'true'::jsonb) IS TRUE AS no_follow,
                   (coalesce(b.r -> 'noArchive', b.r -> 'NoArchive') = 'true'::jsonb) IS TRUE AS no_archive,
                   (coalesce(b.r -> 'noSnippet', b.r -> 'NoSnippet') = 'true'::jsonb) IS TRUE AS no_snippet,
                   (coalesce(b.r -> 'none', b.r -> 'None') = 'true'::jsonb) IS TRUE AS none,
                   btrim(coalesce(b.r ->> 'content', b.r ->> 'Content', '')) AS content
            FROM broken b)
        UPDATE "{{Documents}}" d
        SET "Policy" = jsonb_build_object(
                'followLinks', NOT (x.no_follow OR x.none),
                'index', NOT (x.no_index OR x.none),
                'indexUntil', NULL,
                'snippetMaxLength', CASE WHEN x.no_snippet THEN 0 END,
                'archive', NOT x.no_archive,
                'directives', CASE WHEN x.content <> '' THEN jsonb_build_array(x.content) ELSE '[]'::jsonb END)::text,
            "IndexedAt" = CASE WHEN (x.no_index OR x.no_snippet OR x.none) AND d."Id" IN (SELECT "Id" FROM latest)
                               THEN NULL ELSE x."IndexedAt" END
        FROM flags x
        WHERE d."Id" = x."Id"
        """;

    public const string DeletePreTelemetryEventsSql = $"""
        DELETE FROM "{Events}" WHERE "EventType" = 'FetchCompleted' AND NOT ("Payload"::jsonb ? 'contentStatus')
        """;

    public const string BackfillEventPagesSql = $"""
        UPDATE "{Events}" e
        SET "Payload" = (e."Payload"::jsonb || jsonb_build_object('pageId', u."Id", 'host', u."Host"))::text
        FROM (SELECT DISTINCT ON ("CrawlJobId", "Url") "Id", "CrawlJobId", "Url", "Host"
              FROM "{Urls}" ORDER BY "CrawlJobId", "Url", "Id") u
        WHERE e."EventType" = 'FetchCompleted' AND NOT (e."Payload"::jsonb ? 'pageId')
          AND u."CrawlJobId" = e."CrawlJobId" AND u."Url" = e."Payload"::jsonb ->> 'url'
        """;

    public const string DeleteUnmatchedEventsSql = $"""
        DELETE FROM "{Events}" WHERE "EventType" = 'FetchCompleted' AND NOT ("Payload"::jsonb ? 'pageId')
        """;

    public override void Up()
    {
        Execute.Sql(BackfillPoliciesSql);
        Alter.Column("Policy").OnTable(Documents).AsString().NotNullable();
        Execute.Sql(DeletePreTelemetryEventsSql);
        Execute.Sql(BackfillEventPagesSql);
        Execute.Sql(DeleteUnmatchedEventsSql);
    }

    public override void Down() =>
        throw new NotSupportedException(
            "Migration 029 deleted crawl events that cannot be rebuilt. Restore a database backup taken before it to go back.");
}
