using Microsoft.Extensions.Logging;
using Samobot.Infrastructure.Enums;
using SamoBot.Infrastructure.Data.Abstractions;
using SamoBot.Infrastructure.Services.Abstractions;

namespace SamoBot.Infrastructure.Services;

public class IndexJobService : IIndexJobService
{
    private readonly IIndexJobRepository _indexJobRepository;
    private readonly ILogger<IndexJobService> _logger;

    public IndexJobService(
        IIndexJobRepository indexJobRepository,
        ILogger<IndexJobService> logger)
    {
        _indexJobRepository = indexJobRepository;
        _logger = logger;
    }

    public async Task<int> QueueForIndexing(int documentId, CancellationToken cancellationToken = default)
    {
        if (await _indexJobRepository.HasPendingJob(documentId, IndexJobOperation.Index, cancellationToken))
        {
            _logger.LogDebug("Document {DocumentId} already has a pending Index job, skipping", documentId);
            return 0;
        }

        var jobId = await _indexJobRepository.Create(documentId, IndexJobOperation.Index, cancellationToken);
        _logger.LogDebug("Created Index job {JobId} for document {DocumentId}", jobId, documentId);
        return jobId;
    }

    public async Task<int> QueueForDeletion(int documentId, CancellationToken cancellationToken = default)
    {
        if (await _indexJobRepository.HasPendingJob(documentId, IndexJobOperation.Delete, cancellationToken))
        {
            _logger.LogDebug("Document {DocumentId} already has a pending Delete job, skipping", documentId);
            return 0;
        }

        var jobId = await _indexJobRepository.Create(documentId, IndexJobOperation.Delete, cancellationToken);
        _logger.LogDebug("Created Delete job {JobId} for document {DocumentId}", jobId, documentId);
        return jobId;
    }

    public async Task<int> QueueForReindex(int documentId, CancellationToken cancellationToken = default)
    {
        if (await _indexJobRepository.HasPendingJob(documentId, IndexJobOperation.Reindex, cancellationToken))
        {
            _logger.LogDebug("Document {DocumentId} already has a pending Reindex job, skipping", documentId);
            return 0;
        }

        var jobId = await _indexJobRepository.Create(documentId, IndexJobOperation.Reindex, cancellationToken);
        _logger.LogDebug("Created Reindex job {JobId} for document {DocumentId}", jobId, documentId);
        return jobId;
    }

    public async Task QueueBatch(IEnumerable<int> documentIds, IndexJobOperation operation, CancellationToken cancellationToken = default)
    {
        var jobs = documentIds.Select(id => (id, operation)).ToList();
        if (jobs.Count == 0)
            return;

        await _indexJobRepository.CreateBatch(jobs, cancellationToken);
        _logger.LogInformation("Created {Count} {Operation} jobs", jobs.Count, operation);
    }
}
