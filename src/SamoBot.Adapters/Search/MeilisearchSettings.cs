using Meilisearch;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Search;

public sealed partial class MeilisearchSettings(
    MeilisearchClient client, IOptions<MeilisearchOptions> options, ILogger<MeilisearchSettings> logger) : IDisposable
{
    public static readonly IReadOnlyList<string> RetiredIndexes = ["parsed_documents"];

    public const string HostAttribute = "host";
    public const string IndexUntilAttribute = "indexUntil";

    public static string AvailableFilter(DateTimeOffset now) =>
        $"{IndexUntilAttribute} > {now.ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture)}";

    private readonly SemaphoreSlim _gate = new(1, 1);
    private volatile bool _applied;

    public void Dispose() => _gate.Dispose();

    public async Task Ensure(CancellationToken ct)
    {
        if (_applied)
        {
            return;
        }

        await _gate.WaitAsync(ct);
        try
        {
            if (!_applied)
            {
                await Apply(ct);
                _applied = true;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task Apply(CancellationToken ct)
    {
        var settings = new Settings
        {
            SearchableAttributes = ["title", "description", "keywords", "bodyText"],
            FilterableAttributes = [HostAttribute, IndexUntilAttribute]
        };

        var accepted = await client.Index(options.Value.IndexName).UpdateSettingsAsync(settings, ct);
        var task = await client.WaitForTaskAsync(accepted.TaskUid, cancellationToken: ct);
        if (task.Status == TaskInfoStatus.Failed)
        {
            throw new InvalidOperationException("Meilisearch rejected the index settings.");
        }

        foreach (var retired in RetiredIndexes.Where(name => name != options.Value.IndexName))
        {
            await DeleteRetired(retired, ct);
        }
    }

    private async Task DeleteRetired(string index, CancellationToken ct)
    {
        var accepted = await client.DeleteIndexAsync(index, ct);
        var task = await client.WaitForTaskAsync(accepted.TaskUid, cancellationToken: ct);
        if (task.Status == TaskInfoStatus.Succeeded)
        {
            LogRetiredIndexDeleted(index);
        }
        else if (ErrorCode(task) != "index_not_found")
        {
            LogRetiredIndexKept(index, ErrorCode(task));
        }
    }

    private static string? ErrorCode(TaskResource task) =>
        task.Error is not null && task.Error.TryGetValue("code", out var code) ? code : null;

    [LoggerMessage(EventId = 1, Level = LogLevel.Information, Message = "Deleted the retired search index {Index}.")]
    private partial void LogRetiredIndexDeleted(string index);

    [LoggerMessage(EventId = 2, Level = LogLevel.Warning, Message = "Could not delete the retired search index {Index} ({Code}). It is retried on the next start.")]
    private partial void LogRetiredIndexKept(string index, string? code);
}
