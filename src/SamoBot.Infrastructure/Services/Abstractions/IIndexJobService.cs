using Samobot.Infrastructure.Enums;

namespace SamoBot.Infrastructure.Services.Abstractions;

public interface IIndexJobService
{
    Task<int> QueueForIndexing(int documentId, CancellationToken cancellationToken = default);
    Task<int> QueueForDeletion(int documentId, CancellationToken cancellationToken = default);
    Task<int> QueueForReindex(int documentId, CancellationToken cancellationToken = default);
    Task QueueBatch(IEnumerable<int> documentIds, IndexJobOperation operation, CancellationToken cancellationToken = default);
}
