using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

/// <summary>Turns the query string of the sites and host pages endpoints into read-model queries. A null result means 400.</summary>
public static class ListQueries
{
    private const int MaxLimit = 200;

    public static SiteQuery? Sites(string? query, string? sort, int limit, int offset)
    {
        var parsed = ParseSort(sort);
        return parsed is null ? null : new SiteQuery(Clean(query), parsed.Value, ClampLimit(limit), Math.Max(offset, 0));
    }

    public static PageQuery? Pages(string? query, string? state, int limit, int offset)
    {
        var parsed = ParseState(state);
        return parsed is null ? null : new PageQuery(Clean(query), parsed.Value, ClampLimit(limit), Math.Max(offset, 0));
    }

    private static SiteSort? ParseSort(string? sort) => sort?.ToLowerInvariant() switch
    {
        null or "" or "host" => SiteSort.Host,
        "pages" => SiteSort.Pages,
        "failed" => SiteSort.Failed,
        "lastcrawl" => SiteSort.LastCrawl,
        _ => null
    };

    private static PageStateFilter? ParseState(string? state) => state?.ToLowerInvariant() switch
    {
        null or "" or "all" => PageStateFilter.All,
        "ok" => PageStateFilter.Ok,
        "redirect" => PageStateFilter.Redirect,
        "error" => PageStateFilter.Error,
        _ => null
    };

    private static int ClampLimit(int limit) => Math.Clamp(limit, 1, MaxLimit);

    private static string? Clean(string? query) => string.IsNullOrWhiteSpace(query) ? null : query.Trim();
}
