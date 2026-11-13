using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>The pages that wait for the search index. Indexing is idempotent, so a claim needs no lease.</summary>
public sealed class PostgresIndexBacklog(NpgsqlDataSource dataSource, TimeProvider timeProvider) : IIndexBacklog
{
    private const string ClaimSql = $"""
        SELECT "Id" AS "PageId", "UrlFetchId",
               COALESCE("Title", '') AS "Title", COALESCE("Description", '') AS "Description",
               COALESCE("Keywords", '') AS "Keywords", COALESCE("Author", '') AS "Author",
               COALESCE("Language", '') AS "Language", COALESCE("Canonical", '') AS "Canonical",
               COALESCE("BodyText", '') AS "BodyText"
        FROM "{TableNames.ParsedDocuments}"
        WHERE "IndexedAt" IS NULL
        ORDER BY "Id"
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
