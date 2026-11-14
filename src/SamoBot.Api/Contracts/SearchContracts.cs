using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

/// <summary>The JSON of GET /api/v1/search. Snippet holds encoded text and mark tags only.</summary>
public sealed record SearchHitDto(int PageId, string Host, string Url, string Title, string Snippet, string Language, DateTimeOffset? FetchedAt)
{
    public static SearchHitDto From(SearchResult result) =>
        new(result.PageId, result.Host, result.Url, result.Title, result.Snippet, result.Language, result.FetchedAt);
}

public sealed record SearchHostDto(string Host, int Count);

public sealed record SearchResponse(int Total, IReadOnlyList<SearchHitDto> Items, long TookMs, IReadOnlyList<SearchHostDto> Hosts)
{
    public static SearchResponse From(SearchPage page) => new(
        page.Total,
        page.Items.Select(SearchHitDto.From).ToList(),
        page.TookMs,
        page.Hosts.Select(h => new SearchHostDto(h.Host, h.Count)).ToList());
}
