using System.Data.Common;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Samobot.Infrastructure.Enums;
using SamoBot.Infrastructure.Data;
using SamoBot.Infrastructure.Data.Abstractions;
using SamoBot.Infrastructure.Database;
using SamoBot.Infrastructure.Services.Abstractions;

namespace SamoBot.Workers;

public class IndexerWorker : BackgroundService
{
    private const int BatchSize = 50;
    private const int MaxAttempts = 3;
    private static readonly TimeSpan StaleClaimThreshold = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(30);

    private readonly IServiceProvider _serviceProvider;
    private readonly IDbConnectionFactory _connectionFactory;
    private readonly ILogger<IndexerWorker> _logger;

    public IndexerWorker(
        IServiceProvider serviceProvider,
        IDbConnectionFactory connectionFactory,
        ILogger<IndexerWorker> logger)
    {
        _serviceProvider = serviceProvider;
        _connectionFactory = connectionFactory;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Indexer worker starting...");

        await CreateJobsForDocumentsWithoutJobs(stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CreateJobsForDocumentsWithoutJobs(stoppingToken);
                
                var processed = await ClaimAndProcessBatch(stoppingToken);
                if (processed == 0)
                {
                    await Task.Delay(PollInterval, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in indexer worker");
                await Task.Delay(PollInterval, stoppingToken);
            }
        }

        _logger.LogInformation("Indexer worker stopping...");
    }

    private async Task CreateJobsForDocumentsWithoutJobs(CancellationToken cancellationToken)
    {
        try
        {
            using var scope = _serviceProvider.CreateScope();
            var documentRepository = scope.ServiceProvider.GetRequiredService<IParsedDocumentRepository>();
            var indexJobService = scope.ServiceProvider.GetRequiredService<IIndexJobService>();

            var documentIds = (await documentRepository.GetDocumentIdsWithoutPendingJobs(BatchSize, cancellationToken)).ToList();
            
            if (documentIds.Count > 0)
            {
                await indexJobService.QueueBatch(documentIds, IndexJobOperation.Index, cancellationToken);
                _logger.LogInformation("Created {Count} index jobs for documents without pending jobs", documentIds.Count);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to create jobs for documents without pending jobs");
        }
    }

    private async Task<int> ClaimAndProcessBatch(CancellationToken cancellationToken)
    {
        List<IndexJobEntity> jobs;

        using var connection = _connectionFactory.CreateConnection();
        await ((DbConnection)connection).OpenAsync(cancellationToken);
        using (var transaction = connection.BeginTransaction())
        {
            using var scope = _serviceProvider.CreateScope();
            var jobRepository = scope.ServiceProvider.GetRequiredService<IIndexJobRepository>();
            var staleBefore = DateTimeOffset.UtcNow - StaleClaimThreshold;
            jobs = (await jobRepository.ClaimBatch(BatchSize, staleBefore, transaction, cancellationToken)).ToList();
            transaction.Commit();
        }

        if (jobs.Count == 0)
        {
            return 0;
        }

        _logger.LogDebug("Claimed {Count} index jobs", jobs.Count);

        foreach (var job in jobs)
        {
            await ProcessJob(job, cancellationToken);
        }

        return jobs.Count;
    }

    private async Task ProcessJob(IndexJobEntity job, CancellationToken cancellationToken)
    {
        using var scope = _serviceProvider.CreateScope();
        var jobRepository = scope.ServiceProvider.GetRequiredService<IIndexJobRepository>();
        var indexerService = scope.ServiceProvider.GetRequiredService<IIndexerService>();
        var documentRepository = scope.ServiceProvider.GetRequiredService<IParsedDocumentRepository>();

        try
        {
            var operation = job.GetOperation();

            switch (operation)
            {
                case IndexJobOperation.Index:
                case IndexJobOperation.Reindex:
                    await ProcessIndexOperation(job, indexerService, documentRepository, cancellationToken);
                    break;
                case IndexJobOperation.Delete:
                    await ProcessDeleteOperation(job, indexerService, cancellationToken);
                    break;
                default:
                    _logger.LogWarning("Unknown operation {Operation} for job {JobId}", job.Operation, job.Id);
                    await jobRepository.MarkFailed(job.Id, $"Unknown operation: {job.Operation}", MaxAttempts, cancellationToken);
                    return;
            }

            await jobRepository.MarkCompleted(job.Id, cancellationToken);
            _logger.LogDebug("Job {JobId} completed successfully", job.Id);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Job {JobId} failed: {Error}", job.Id, ex.Message);
            await jobRepository.MarkFailed(job.Id, ex.Message, MaxAttempts, cancellationToken);
        }
    }

    private async Task ProcessIndexOperation(
        IndexJobEntity job,
        IIndexerService indexerService,
        IParsedDocumentRepository documentRepository,
        CancellationToken cancellationToken)
    {
        var documents = await documentRepository.GetByIds([job.DocumentId], cancellationToken);
        var documentList = documents.ToList();

        if (documentList.Count == 0)
        {
            _logger.LogWarning("Document {DocumentId} not found for job {JobId}", job.DocumentId, job.Id);
            throw new InvalidOperationException($"Document {job.DocumentId} not found");
        }

        await indexerService.Index(documentList, cancellationToken);
    }

    private async Task ProcessDeleteOperation(
        IndexJobEntity job,
        IIndexerService indexerService,
        CancellationToken cancellationToken)
    {
        await indexerService.Delete(new[] { job.DocumentId }, cancellationToken);
    }
}
