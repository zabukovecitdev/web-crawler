using System.Data;
using Samobot.Infrastructure.Enums;

namespace SamoBot.Infrastructure.Data.Abstractions;

public interface IIndexJobRepository
{
    Task<IndexJobEntity?> GetById(int id, CancellationToken cancellationToken = default);
    Task<IEnumerable<IndexJobEntity>> GetByDocumentId(int documentId, CancellationToken cancellationToken = default);
    Task<int> Create(int documentId, IndexJobOperation operation, CancellationToken cancellationToken = default);
    Task CreateBatch(IEnumerable<(int DocumentId, IndexJobOperation Operation)> jobs, CancellationToken cancellationToken = default);
    Task<IEnumerable<IndexJobEntity>> ClaimBatch(int limit, DateTimeOffset staleBefore, IDbTransaction transaction, CancellationToken cancellationToken = default);
    Task MarkCompleted(int jobId, CancellationToken cancellationToken = default);
    Task MarkFailed(int jobId, string error, int maxAttempts, CancellationToken cancellationToken = default);
    Task<bool> HasPendingJob(int documentId, IndexJobOperation operation, CancellationToken cancellationToken = default);
}
