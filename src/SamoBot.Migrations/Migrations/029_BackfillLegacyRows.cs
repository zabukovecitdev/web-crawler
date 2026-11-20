using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000029, "Backfill page policies and event page ids so readers handle one shape")]
public class BackfillLegacyRows : Migration
{
    public const string DefaultPolicyJson =
        """{"followLinks":true,"index":true,"indexUntil":null,"snippetMaxLength":null,"archive":true,"directives":[]}""";

    private const string Documents = TableNames.Database.ParsedDocuments;
    private const string Events = TableNames.Database.CrawlJobEvents;
    private const string Urls = TableNames.Database.DiscoveredUrls;

    public const string BackfillPoliciesSql = $$"""
        UPDATE "{{Documents}}"
        SET "Policy" = CASE
            WHEN "RobotsDirectives" IS NOT NULL AND pg_input_is_valid("RobotsDirectives", 'jsonb')
                 AND coalesce(btrim("RobotsDirectives"::jsonb ->> 'content'), '') <> ''
            THEN jsonb_set('{{DefaultPolicyJson}}'::jsonb, '{directives}',
                           jsonb_build_array(btrim("RobotsDirectives"::jsonb ->> 'content')))::text
            ELSE '{{DefaultPolicyJson}}'
        END
        WHERE "Policy" IS NULL
        """;

    public const string BackfillEventPagesSql = $"""
        UPDATE "{Events}" e
        SET "Payload" = (e."Payload"::jsonb || jsonb_build_object('pageId', u."Id", 'host', u."Host"))::text
        FROM "{Urls}" u
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
        Delete.Column("RobotsDirectives").FromTable(Documents);
        Execute.Sql(BackfillEventPagesSql);
        Execute.Sql(DeleteUnmatchedEventsSql);
    }

    public override void Down()
    {
        Alter.Table(Documents).AddColumn("RobotsDirectives").AsString().Nullable();
        Alter.Column("Policy").OnTable(Documents).AsString().Nullable();
    }
}
