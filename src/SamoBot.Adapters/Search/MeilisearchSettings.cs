using Meilisearch;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Search;

/// <summary>
/// Applies the index settings once per process, before the first read or write. Applying the same settings again is a no-op
/// for Meilisearch, so every service that touches the index can call <see cref="Ensure"/>. A failure is retried on the next call.
/// </summary>
public sealed class MeilisearchSettings(MeilisearchClient client, IOptions<MeilisearchOptions> options) : IDisposable
{
    public const string HostAttribute = "host";

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
            FilterableAttributes = [HostAttribute]
        };

        var accepted = await client.Index(options.Value.IndexName).UpdateSettingsAsync(settings, ct);
        var task = await client.WaitForTaskAsync(accepted.TaskUid, cancellationToken: ct);
        if (task.Status == TaskInfoStatus.Failed)
        {
            throw new InvalidOperationException("Meilisearch rejected the index settings.");
        }
    }
}
