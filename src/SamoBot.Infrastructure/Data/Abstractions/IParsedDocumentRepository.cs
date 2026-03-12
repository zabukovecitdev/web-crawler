using SamoBot.Infrastructure.Models;

namespace SamoBot.Infrastructure.Data.Abstractions;

public interface IParsedDocumentRepository
{
    Task<int> SaveParsedDocument(int urlFetchId, ParsedDocument parsedDocument, CancellationToken cancellationToken = default);
    Task<ParsedDocument?> GetByUrlFetchId(int urlFetchId, CancellationToken cancellationToken = default);
    Task<IEnumerable<ParsedDocumentEntity>> GetByIds(IEnumerable<int> ids, CancellationToken cancellationToken = default);
    
    /// <summary>
    /// Gets IDs of parsed documents that don't have any pending or in-progress index jobs.
    /// </summary>
    Task<IEnumerable<int>> GetDocumentIdsWithoutPendingJobs(int limit, CancellationToken cancellationToken = default);
}
