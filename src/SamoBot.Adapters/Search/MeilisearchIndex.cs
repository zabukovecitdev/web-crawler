using System.Globalization;
using Meilisearch;
using Microsoft.Extensions.Options;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Search;

/// <summary>The Meilisearch index of parsed pages. It writes with <see cref="ISearchIndex"/> and reads with <see cref="ISearchQuery"/>.</summary>
public sealed class MeilisearchIndex(MeilisearchClient client, IOptions<MeilisearchOptions> options) : ISearchIndex, ISearchQuery
{
    private Meilisearch.Index Index => client.Index(options.Value.IndexName);

    public async Task AddOrUpdate(IReadOnlyList<PageToIndex> pages, CancellationToken ct)
    {
        if (pages.Count == 0)
        {
            return;
        }

        var documents = pages.Select(ToDocument).ToList();
        var accepted = await Index.AddDocumentsAsync(documents, primaryKey: "id", cancellationToken: ct);
        var task = await client.WaitForTaskAsync(accepted.TaskUid, cancellationToken: ct);
        if (task.Status == TaskInfoStatus.Failed)
        {
            var message = task.Error is not null && task.Error.TryGetValue("message", out var text) ? text : "unknown error";
            throw new InvalidOperationException($"Meilisearch indexing failed: {message}");
        }
    }

    public async Task<IReadOnlyList<SearchHit>> Search(string text, int limit, CancellationToken ct)
    {
        var result = await Index.SearchAsync<SearchHit>(text, new SearchQuery { Limit = limit }, ct);
        return result.Hits.ToList();
    }

    private static SearchHit ToDocument(PageToIndex page) => new()
    {
        Id = page.PageId.ToString(CultureInfo.InvariantCulture),
        UrlFetchId = page.UrlFetchId,
        Title = page.Title,
        Description = page.Description,
        Keywords = page.Keywords,
        Author = page.Author,
        Language = page.Language,
        Canonical = page.Canonical,
        BodyText = page.BodyText
    };
}
