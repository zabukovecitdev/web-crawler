using Meilisearch;
using Microsoft.Extensions.Options;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Core.Search;

namespace SamoBot.Adapters.Search;

public sealed class MeilisearchPageSearch(
    MeilisearchClient client,
    IOptions<MeilisearchOptions> options,
    MeilisearchSettings settings,
    TimeProvider timeProvider) : ISearchPages
{
    private const int CropWords = 30;

    private Meilisearch.Index Index => client.Index(options.Value.IndexName);

    public async Task<SearchPage> Search(SearchRequest request, CancellationToken ct)
    {
        await settings.Ensure(ct);
        var result = (SearchResult<IndexedHit>)await Index.SearchAsync<IndexedHit>(request.Text, Query(request), ct);
        var hosts = request.Host is null ? FacetHosts(result) : await AllHosts(request.Text, ct);
        return new SearchPage(result.EstimatedTotalHits, result.ProcessingTimeMs, hosts, result.Hits.Select(ToResult).ToList());
    }

    private async Task<IReadOnlyList<HostCount>> AllHosts(string text, CancellationToken ct)
    {
        var query = new Meilisearch.SearchQuery
        {
            Limit = 0,
            Facets = [MeilisearchSettings.HostAttribute],
            Filter = MeilisearchSettings.AvailableFilter(timeProvider.GetUtcNow())
        };
        return FacetHosts(await Index.SearchAsync<IndexedHit>(text, query, ct));
    }

    private Meilisearch.SearchQuery Query(SearchRequest request) => new()
    {
        Limit = request.Limit,
        Offset = request.Offset,
        Filter = Filter(request.Host, timeProvider.GetUtcNow()),
        Facets = [MeilisearchSettings.HostAttribute],
        AttributesToHighlight = ["bodyText", "description"],
        AttributesToCrop = ["bodyText", "description"],
        CropLength = CropWords,
        HighlightPreTag = SnippetFormatter.HighlightStart.ToString(),
        HighlightPostTag = SnippetFormatter.HighlightEnd.ToString()
    };

    public static string Filter(string? host, DateTimeOffset now) =>
        host is null ? MeilisearchSettings.AvailableFilter(now) : $"{MeilisearchSettings.AvailableFilter(now)} AND {HostFilter(host)}";

    public static string HostFilter(string host) =>
        $"{MeilisearchSettings.HostAttribute} = \"{host.Replace("\\", "\\\\").Replace("\"", "\\\"")}\"";

    private static List<HostCount> FacetHosts(ISearchable<IndexedHit> result) =>
        result.FacetDistribution is not null && result.FacetDistribution.TryGetValue(MeilisearchSettings.HostAttribute, out var counts)
            ? counts.Select(pair => new HostCount(pair.Key, pair.Value)).OrderByDescending(h => h.Count).ThenBy(h => h.Host, StringComparer.Ordinal).ToList()
            : [];

    private static Core.Domain.SearchResult ToResult(IndexedHit hit) => new(
        hit.DiscoveredUrlId,
        hit.Host,
        hit.Url,
        hit.Title,
        SnippetFormatter.PickWithin(hit.SnippetMaxLength < 0 ? null : hit.SnippetMaxLength, hit.Formatted?.BodyText, hit.Formatted?.Description),
        hit.Language,
        hit.FetchedAt);
}
