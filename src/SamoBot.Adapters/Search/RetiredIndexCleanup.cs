using Meilisearch;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Search;

public sealed partial class RetiredIndexCleanup(
    MeilisearchClient client, IOptions<MeilisearchOptions> options, ILogger<RetiredIndexCleanup> logger) : BackgroundService
{
    public static readonly IReadOnlyList<string> RetiredIndexes = ["parsed_documents"];

    private static readonly TimeSpan DeleteTimeout = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan PollInterval = TimeSpan.FromSeconds(1);

    public async Task Run(CancellationToken ct)
    {
        foreach (var index in RetiredIndexes.Where(name => name != options.Value.IndexName))
        {
            await Delete(index, ct);
        }
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken) => Run(stoppingToken);

    private async Task Delete(string index, CancellationToken ct)
    {
        try
        {
            Report(index, await DeleteAndWait(index, ct));
        }
        catch (Exception ex) when (ex is MeilisearchApiError or MeilisearchCommunicationError or MeilisearchTimeoutError or HttpRequestException)
        {
            LogRetiredIndexKept(index, ex.Message);
        }
    }

    private async Task<TaskResource> DeleteAndWait(string index, CancellationToken ct)
    {
        var accepted = await client.DeleteIndexAsync(index, ct);
        return await client.WaitForTaskAsync(
            accepted.TaskUid, DeleteTimeout.TotalMilliseconds, (int)PollInterval.TotalMilliseconds, ct);
    }

    private void Report(string index, TaskResource task)
    {
        var code = task.Error is not null && task.Error.TryGetValue("code", out var value) ? value : null;
        if (task.Status == TaskInfoStatus.Succeeded)
        {
            LogRetiredIndexDeleted(index);
        }
        else if (code != "index_not_found")
        {
            LogRetiredIndexKept(index, code ?? $"task {task.Status}");
        }
    }

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Deleted the retired search index {Index}.")]
    private partial void LogRetiredIndexDeleted(string index);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Warning,
        Message = "Could not delete the retired search index {Index}: {Reason}. It is tried again on the next start.")]
    private partial void LogRetiredIndexKept(string index, string reason);
}
