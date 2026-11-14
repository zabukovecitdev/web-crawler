using Meilisearch;
using Microsoft.Extensions.Options;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Core.Search;

namespace SamoBot.Adapters.Search;

/// <summary>The dashboard search over the Meilisearch index: highlighted snippets, a host filter and the match count per host.</summary>
public sealed class MeilisearchPageSearch(MeilisearchClient client, IOptions<MeilisearchOptions> options, MeilisearchSettings settings) : ISearchPages
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

    /// <summary>The host list ignores the host filter, so the dashboard can still offer the other hosts.</summary>
    private async Task<IReadOnlyList<HostCount>> AllHosts(string text, CancellationToken ct)
    {
        var query = new Meilisearch.SearchQuery { Limit = 0, Facets = [MeilisearchSettings.HostAttribute] };
        return FacetHosts(await Index.SearchAsync<IndexedHit>(text, query, ct));
    }

    private static Meilisearch.SearchQuery Query(SearchRequest request) => new()
    {
        Limit = request.Limit,
        Offset = request.Offset,
        Filter = request.Host is null ? null : HostFilter(request.Host),
        Facets = [MeilisearchSettings.HostAttribute],
        AttributesToHighlight = ["bodyText", "description"],
        AttributesToCrop = ["bodyText", "description"],
        CropLength = CropWords,
        HighlightPreTag = SnippetFormatter.HighlightStart.ToString(),
        HighlightPostTag = SnippetFormatter.HighlightEnd.ToString()
    };

    /// <summary>The filter value is a quoted string, so a backslash or a quote in a host cannot end it early.</summary>
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
        SnippetFormatter.Pick(hit.Formatted?.BodyText, hit.Formatted?.Description),
        hit.Language,
        hit.FetchedAt);
}
