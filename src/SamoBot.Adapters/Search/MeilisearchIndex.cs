using System.Globalization;
using Meilisearch;
using Microsoft.Extensions.Options;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Search;

public sealed class MeilisearchIndex(
    MeilisearchClient client,
    IOptions<MeilisearchOptions> options,
    MeilisearchSettings settings,
    TimeProvider timeProvider) : ISearchIndex, ISearchQuery
{
    private Meilisearch.Index Index => client.Index(options.Value.IndexName);

    public async Task AddOrUpdate(IReadOnlyList<PageToIndex> pages, CancellationToken ct)
    {
        if (pages.Count == 0)
        {
            return;
        }

        await settings.Ensure(ct);
        var documents = LatestPerUrl(pages).Select(ToDocument).ToList();
        var accepted = await Index.AddDocumentsAsync(documents, primaryKey: "id", cancellationToken: ct);
        await Wait(accepted.TaskUid, "indexing", ct);
    }

    public async Task Remove(IReadOnlyList<int> discoveredUrlIds, CancellationToken ct)
    {
        if (discoveredUrlIds.Count == 0)
        {
            return;
        }

        await settings.Ensure(ct);
        var ids = discoveredUrlIds.Distinct().Select(id => id.ToString(CultureInfo.InvariantCulture));
        var accepted = await Index.DeleteDocumentsAsync(ids, ct);
        await Wait(accepted.TaskUid, "removing documents", ct);
    }

    public async Task<IReadOnlyList<SearchHit>> Search(string text, int limit, CancellationToken ct)
    {
        await settings.Ensure(ct);
        var query = new SearchQuery { Limit = limit, Filter = MeilisearchSettings.AvailableFilter(timeProvider.GetUtcNow()) };
        var result = await Index.SearchAsync<SearchHit>(text, query, ct);
        return result.Hits.ToList();
    }

    private async Task Wait(int taskUid, string action, CancellationToken ct)
    {
        var task = await client.WaitForTaskAsync(taskUid, cancellationToken: ct);
        if (task.Status == TaskInfoStatus.Failed)
        {
            var message = task.Error is not null && task.Error.TryGetValue("message", out var text) ? text : "unknown error";
            throw new InvalidOperationException($"Meilisearch {action} failed: {message}");
        }
    }

    internal static IReadOnlyList<PageToIndex> LatestPerUrl(IReadOnlyList<PageToIndex> pages) =>
        pages.GroupBy(p => p.DiscoveredUrlId).Select(g => g.MaxBy(p => p.PageId)!).ToList();

    internal static SearchHit ToDocument(PageToIndex page) => new()
    {
        Id = page.DiscoveredUrlId.ToString(CultureInfo.InvariantCulture),
        UrlFetchId = page.UrlFetchId,
        DiscoveredUrlId = page.DiscoveredUrlId,
        Host = page.Host,
        Url = page.Url,
        FetchedAt = page.FetchedAt,
        Title = page.Title,
        Description = page.Description,
        Keywords = page.Keywords,
        Author = page.Author,
        Language = page.Language,
        Canonical = page.Canonical,
        BodyText = page.BodyText,
        IndexUntil = page.Policy.IndexUntil?.ToUnixTimeSeconds() ?? SearchHit.NoEnd,
        SnippetMaxLength = page.Policy.SnippetMaxLength ?? -1
    };
}
