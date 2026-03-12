using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Meilisearch;
using SamoBot.Infrastructure.Data;
using SamoBot.Infrastructure.Models;
using SamoBot.Infrastructure.Options;
using SamoBot.Infrastructure.Services.Abstractions;

namespace SamoBot.Infrastructure.Services;

public class IndexService : IIndexerService
{
    private readonly MeilisearchClient _meilisearchClient;
    private readonly MeilisearchOptions _options;
    private readonly ILogger<IndexService> _logger;

    public IndexService(
        MeilisearchClient meilisearchClient,
        IOptions<MeilisearchOptions> options,
        ILogger<IndexService> logger)
    {
        _meilisearchClient = meilisearchClient;
        _options = options.Value;
        _logger = logger;
    }

    public async Task Index(IEnumerable<ParsedDocumentEntity> documents, CancellationToken cancellationToken = default)
    {
        var list = documents.ToList();
        if (list.Count == 0)
            return;

        var meilisearchDocs = list.Select(e => new MeilisearchDocument
        {
            Id = e.Id.ToString(),
            UrlFetchId = e.UrlFetchId,
            Title = e.Title ?? string.Empty,
            Description = e.Description ?? string.Empty,
            Keywords = e.Keywords ?? string.Empty,
            Author = e.Author ?? string.Empty,
            Language = e.Language ?? string.Empty,
            Canonical = e.Canonical ?? string.Empty,
            BodyText = e.BodyText ?? string.Empty
        }).ToList();

        try
        {
            var index = _meilisearchClient.Index(_options.IndexName);
            var taskInfo = await index.AddDocumentsAsync(meilisearchDocs, primaryKey: "id", cancellationToken: cancellationToken);
            
            var task = await _meilisearchClient.WaitForTaskAsync(taskInfo.TaskUid, cancellationToken: cancellationToken);
            if (task.Status == TaskInfoStatus.Failed)
            {
                var errorMessage = task.Error != null && task.Error.TryGetValue("message", out var msg) ? msg : "Unknown error";
                throw new InvalidOperationException($"Meilisearch indexing failed: {errorMessage}");
            }
            
            _logger.LogInformation("Indexed {Count} documents in Meilisearch", list.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Meilisearch unreachable or error; skipping index of {Count} documents", list.Count);
            throw;
        }
    }

    public async Task Delete(IEnumerable<int> documentIds, CancellationToken cancellationToken = default)
    {
        var ids = documentIds.ToList();
        if (ids.Count == 0)
            return;

        try
        {
            var index = _meilisearchClient.Index(_options.IndexName);
            var stringIds = ids.Select(id => id.ToString()).ToList();
            var taskInfo = await index.DeleteDocumentsAsync(stringIds, cancellationToken);
            
            var task = await _meilisearchClient.WaitForTaskAsync(taskInfo.TaskUid, cancellationToken: cancellationToken);
            if (task.Status == TaskInfoStatus.Failed)
            {
                var errorMessage = task.Error != null && task.Error.TryGetValue("message", out var msg) ? msg : "Unknown error";
                throw new InvalidOperationException($"Meilisearch deletion failed: {errorMessage}");
            }
            
            _logger.LogInformation("Deleted {Count} documents from Meilisearch", ids.Count);
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Meilisearch unreachable or error; failed to delete {Count} documents", ids.Count);
            throw;
        }
    }
}