using System.Text.Json;
using Dapper;
using Samobot.Infrastructure.Enums;
using SamoBot.Infrastructure.Constants;
using SamoBot.Infrastructure.Data.Abstractions;
using SamoBot.Infrastructure.Models;
using SqlKata.Execution;

namespace SamoBot.Infrastructure.Data;

public class ParsedDocumentRepository(QueryFactory queryFactory) : IParsedDocumentRepository
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false
    };

    public async Task<int> SaveParsedDocument(int urlFetchId, ParsedDocument parsedDocument, CancellationToken cancellationToken = default)
    {
        var id = await queryFactory.Query(TableNames.Database.ParsedDocuments)
            .InsertGetIdAsync<int>(new
            {
                UrlFetchId = urlFetchId,
                parsedDocument.Title,
                parsedDocument.Description,
                parsedDocument.Keywords,
                parsedDocument.Author,
                parsedDocument.Language,
                parsedDocument.Canonical,
                parsedDocument.BodyText,
                Headings = JsonSerializer.Serialize(parsedDocument.Headings, JsonOptions),
                Images = JsonSerializer.Serialize(parsedDocument.Images, JsonOptions),
                RobotsDirectives = JsonSerializer.Serialize(parsedDocument.RobotsDirectives, JsonOptions),
                OpenGraphData = JsonSerializer.Serialize(parsedDocument.OpenGraphData, JsonOptions),
                TwitterCardData = JsonSerializer.Serialize(parsedDocument.TwitterCardData, JsonOptions),
                JsonLdData = JsonSerializer.Serialize(parsedDocument.JsonLdData, JsonOptions),
                ParsedAt = DateTimeOffset.UtcNow
            }, cancellationToken: cancellationToken);

        return id;
    }

    public async Task<ParsedDocument?> GetByUrlFetchId(int urlFetchId, CancellationToken cancellationToken = default)
    {
        var entity = await queryFactory.Query(TableNames.Database.ParsedDocuments)
            .Where("UrlFetchId", urlFetchId)
            .FirstOrDefaultAsync<ParsedDocumentEntity>(cancellationToken: cancellationToken);

        return entity?.ToParsedDocument();
    }

    public async Task<IEnumerable<ParsedDocumentEntity>> GetByIds(IEnumerable<int> ids, CancellationToken cancellationToken = default)
    {
        var idList = ids.ToList();
        if (idList.Count == 0)
            return Enumerable.Empty<ParsedDocumentEntity>();

        return await queryFactory.Query(TableNames.Database.ParsedDocuments)
            .WhereIn(nameof(ParsedDocumentEntity.Id), idList)
            .GetAsync<ParsedDocumentEntity>(cancellationToken: cancellationToken);
    }

    public async Task<IEnumerable<int>> GetDocumentIdsWithoutPendingJobs(int limit, CancellationToken cancellationToken = default)
    {
        var indexOperation = IndexJobOperation.Index.AsString();

        const string sql = """
            SELECT pd."Id"
            FROM "ParsedDocuments" pd
            WHERE NOT EXISTS (
                SELECT 1 FROM "IndexJobs" ij
                WHERE ij."DocumentId" = pd."Id"
                AND ij."Operation" = @IndexOperation
            )
            ORDER BY pd."Id"
            LIMIT @Limit
            """;

        var connection = queryFactory.Connection;
        return await connection.QueryAsync<int>(
            new CommandDefinition(sql, new { IndexOperation = indexOperation, Limit = limit }, cancellationToken: cancellationToken));
    }
}
