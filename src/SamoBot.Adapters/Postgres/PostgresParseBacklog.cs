using System.Text.Json;
using Dapper;
using Npgsql;
using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>The parse work queue. A fetch is claimed with a lease. One transaction stores the page, its links and the parsed mark.</summary>
public sealed class PostgresParseBacklog(NpgsqlDataSource dataSource, TimeProvider timeProvider) : IParseBacklog
{
    private const string ClaimSql = $"""
        WITH picked AS (
            SELECT f."Id"
            FROM "{TableNames.UrlFetches}" f
            JOIN "{TableNames.DiscoveredUrls}" u ON u."Id" = f."DiscoveredUrlId"
            JOIN "{TableNames.CrawlJobs}" j ON j."Id" = u."CrawlJobId"
            WHERE f."ParsedAt" IS NULL AND f."ObjectName" IS NOT NULL
              AND (f."LeaseUntil" IS NULL OR f."LeaseUntil" < @Now)
            ORDER BY f."Id"
            LIMIT @BatchSize
            FOR UPDATE OF f SKIP LOCKED
        ),
        leased AS (
            UPDATE "{TableNames.UrlFetches}" f
            SET "LeaseUntil" = @Now + make_interval(secs => @LeaseSeconds)
            FROM picked p WHERE p."Id" = f."Id"
            RETURNING f."Id", f."DiscoveredUrlId", f."ObjectName"
        )
        SELECT l."Id" AS "FetchId", u."Id" AS "UrlId", u."CrawlJobId", u."Url", u."Depth", l."ObjectName",
               j."AllowedHosts", j."MaxDepth"
        FROM leased l
        JOIN "{TableNames.DiscoveredUrls}" u ON u."Id" = l."DiscoveredUrlId"
        JOIN "{TableNames.CrawlJobs}" j ON j."Id" = u."CrawlJobId"
        """;

    private const string UpsertPageSql = $"""
        INSERT INTO "{TableNames.ParsedDocuments}"
            ("UrlFetchId", "Title", "Description", "Keywords", "Author", "Language", "Canonical", "BodyText",
             "Headings", "Images", "RobotsDirectives", "OpenGraphData", "TwitterCardData", "JsonLdData", "ParsedAt", "IndexedAt")
        VALUES
            (@FetchId, @Title, @Description, @Keywords, @Author, @Language, @Canonical, @BodyText,
             @Headings, @Images, @RobotsDirectives, @OpenGraphData, @TwitterCardData, @JsonLdData, @Now, NULL)
        ON CONFLICT ("UrlFetchId") DO UPDATE SET
            "Title" = EXCLUDED."Title", "Description" = EXCLUDED."Description", "Keywords" = EXCLUDED."Keywords",
            "Author" = EXCLUDED."Author", "Language" = EXCLUDED."Language", "Canonical" = EXCLUDED."Canonical",
            "BodyText" = EXCLUDED."BodyText", "Headings" = EXCLUDED."Headings", "Images" = EXCLUDED."Images",
            "RobotsDirectives" = EXCLUDED."RobotsDirectives", "OpenGraphData" = EXCLUDED."OpenGraphData",
            "TwitterCardData" = EXCLUDED."TwitterCardData", "JsonLdData" = EXCLUDED."JsonLdData",
            "ParsedAt" = EXCLUDED."ParsedAt", "IndexedAt" = NULL
        """;

    private const string MarkParsedSql = $"""
        UPDATE "{TableNames.UrlFetches}" SET "ParsedAt" = @Now, "LeaseUntil" = NULL WHERE "Id" = @FetchId
        """;

    static PostgresParseBacklog() => DapperTypeHandlers.Register();

    public async Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { Now = timeProvider.GetUtcNow(), BatchSize = batchSize, LeaseSeconds = lease.TotalSeconds };
        var fetches = await connection.QueryAsync<FetchToParse>(new CommandDefinition(ClaimSql, parameters, cancellationToken: ct));
        return fetches.ToList();
    }

    public async Task Complete(FetchToParse fetch, ParsedPage page, IReadOnlyList<NewUrl> links, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var now = timeProvider.GetUtcNow();

        await connection.ExecuteAsync(new CommandDefinition(UpsertPageSql, PageParameters(fetch, page, now), transaction, cancellationToken: ct));
        await NewUrlInserter.Insert(connection, transaction, fetch.CrawlJobId, links, now, ct);
        await connection.ExecuteAsync(new CommandDefinition(MarkParsedSql, new { fetch.FetchId, Now = now }, transaction, cancellationToken: ct));
        await transaction.CommitAsync(ct);
    }

    private static object PageParameters(FetchToParse fetch, ParsedPage page, DateTimeOffset now) => new
    {
        fetch.FetchId,
        Title = Truncate(page.Title, 500),
        Description = Truncate(page.Description, 2000),
        Keywords = Truncate(page.Keywords, 1000),
        Author = Truncate(page.Author, 255),
        Language = Truncate(page.Language, 10),
        Canonical = Truncate(page.Canonical, 2048),
        page.BodyText,
        Headings = Json(page.Headings),
        Images = Json(page.Images),
        RobotsDirectives = Json(page.RobotsDirectives),
        OpenGraphData = Json(page.OpenGraphData),
        TwitterCardData = Json(page.TwitterCardData),
        JsonLdData = Json(page.JsonLdData),
        Now = now
    };

    private static string Json<T>(T value) => JsonSerializer.Serialize(value, JsonDefaults.Options);

    private static string Truncate(string value, int maxLength) =>
        value.Length <= maxLength ? value : value[..maxLength];
}
