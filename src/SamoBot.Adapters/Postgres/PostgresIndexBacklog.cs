using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>The pages that wait for the search index. Indexing is idempotent, so a claim needs no lease.</summary>
public sealed class PostgresIndexBacklog(NpgsqlDataSource dataSource, TimeProvider timeProvider) : IIndexBacklog
{
    private const string ClaimSql = $"""
        SELECT d."Id" AS "PageId", d."UrlFetchId", u."Id" AS "DiscoveredUrlId", u."Host", u."Url", f."FetchedAt",
               COALESCE(d."Title", '') AS "Title", COALESCE(d."Description", '') AS "Description",
               COALESCE(d."Keywords", '') AS "Keywords", COALESCE(d."Author", '') AS "Author",
               COALESCE(d."Language", '') AS "Language", COALESCE(d."Canonical", '') AS "Canonical",
               COALESCE(d."BodyText", '') AS "BodyText"
        FROM "{TableNames.ParsedDocuments}" d
        JOIN "{TableNames.UrlFetches}" f ON f."Id" = d."UrlFetchId"
        JOIN "{TableNames.DiscoveredUrls}" u ON u."Id" = f."DiscoveredUrlId"
        WHERE d."IndexedAt" IS NULL
        ORDER BY d."Id"
        LIMIT @BatchSize
        """;

    private const string MarkSql = $"""
        UPDATE "{TableNames.ParsedDocuments}" SET "IndexedAt" = @Now WHERE "Id" = ANY(@PageIds)
        """;

    public async Task<IReadOnlyList<PageToIndex>> Claim(int batchSize, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var pages = await connection.QueryAsync<PageToIndex>(new CommandDefinition(ClaimSql, new { BatchSize = batchSize }, cancellationToken: ct));
        return pages.ToList();
    }

    public async Task MarkIndexed(IReadOnlyList<int> pageIds, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { PageIds = pageIds.ToArray(), Now = timeProvider.GetUtcNow() };
        await connection.ExecuteAsync(new CommandDefinition(MarkSql, parameters, cancellationToken: ct));
    }
}
