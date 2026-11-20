using Meilisearch;
using Microsoft.Extensions.Options;

namespace SamoBot.Adapters.Search;

public sealed class MeilisearchSettings(MeilisearchClient client, IOptions<MeilisearchOptions> options) : IDisposable
{
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
    }
}
