using FluentMigrator;
using SamoBot.Migrations.Constants;

namespace SamoBot.Migrations.Migrations;

[Migration(20240101000014, "Redesign the frontier: leases, per-job uniqueness, Hosts table, IndexedAt, drop IndexJobs")]
public class RedesignFrontier : Migration
{
    private const string UrlsTable = TableNames.Database.DiscoveredUrls;
    private const string FetchesTable = TableNames.Database.UrlFetches;
    private const string DocumentsTable = TableNames.Database.ParsedDocuments;
    private const string JobsTable = TableNames.Database.CrawlJobs;
    private const string HostsTable = TableNames.Database.Hosts;

    public override void Up()
    {
        ExtendDiscoveredUrls();
        CreateHostsTable();
        ExtendUrlFetches();
        ReplaceIndexJobsWithIndexedAt();
        AddAllowedHostsToJobs();
    }

    private void ExtendDiscoveredUrls()
    {
        Alter.Table(UrlsTable)
            .AddColumn("LeaseUntil").AsDateTimeOffset().Nullable()
            .AddColumn("FailReason").AsString().Nullable();

        Execute.Sql($@"ALTER TABLE ""{UrlsTable}"" ALTER COLUMN ""NextCrawlAt"" DROP NOT NULL;");

        Delete.UniqueConstraint("IX_DiscoveredUrls_NormalizedUrl_Unique").FromTable(UrlsTable);
        Create.UniqueConstraint("UQ_DiscoveredUrls_CrawlJobId_NormalizedUrl")
            .OnTable(UrlsTable)
            .Columns("CrawlJobId", "NormalizedUrl");

        Create.Index("IX_DiscoveredUrls_Host_Status_NextCrawlAt")
            .OnTable(UrlsTable)
            .OnColumn("Host").Ascending()
            .OnColumn("Status").Ascending()
            .OnColumn("NextCrawlAt").Ascending();
    }

    private void CreateHostsTable()
    {
        Create.Table(HostsTable)
            .WithColumn("Host").AsString().NotNullable().PrimaryKey()
            .WithColumn("NextAllowedAt").AsDateTimeOffset().NotNullable().WithDefault(SystemMethods.CurrentDateTime)
            .WithColumn("CrawlDelayMs").AsInt32().NotNullable().WithDefaultValue(1000);

        Execute.Sql($@"INSERT INTO ""{HostsTable}"" (""Host"") SELECT DISTINCT ""Host"" FROM ""{UrlsTable}"" ON CONFLICT DO NOTHING;");
    }

    private void ExtendUrlFetches()
    {
        Alter.Table(FetchesTable)
            .AddColumn("LeaseUntil").AsDateTimeOffset().Nullable()
            .AddColumn("FinalUrl").AsString().Nullable();
    }

    private void ReplaceIndexJobsWithIndexedAt()
    {
        Alter.Table(DocumentsTable).AddColumn("IndexedAt").AsDateTimeOffset().Nullable();

        Execute.Sql($@"
            UPDATE ""{DocumentsTable}"" d SET ""IndexedAt"" = now()
            WHERE EXISTS (
                SELECT 1 FROM ""{TableNames.Database.IndexJobs}"" j
                WHERE j.""DocumentId"" = d.""Id"" AND j.""Status"" = 'Completed');");

        Delete.Table(TableNames.Database.IndexJobs);
    }

    private void AddAllowedHostsToJobs()
    {
        Alter.Table(JobsTable).AddColumn("AllowedHosts").AsString().NotNullable().WithDefaultValue("[]");

        Execute.Sql($@"
            UPDATE ""{JobsTable}"" j SET ""AllowedHosts"" = COALESCE((
                SELECT jsonb_agg(DISTINCT h)::text FROM (
                    SELECT lower(substring(s FROM '^https?://([^/:?#]+)')) AS h
                    FROM jsonb_array_elements_text(j.""SeedUrls""::jsonb) AS s
                ) hosts WHERE h IS NOT NULL), '[]');");
    }

    public override void Down() =>
        throw new NotSupportedException(
            "Migration 014 dropped IndexJobs with its rows and allows one normalized URL in several jobs, which the old unique key "
            + "forbids. Restore a database backup taken before it to go back.");
}
