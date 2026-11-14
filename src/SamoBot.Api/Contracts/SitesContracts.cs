using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

/// <summary>Caps on the list endpoints, so a request cannot make the database scan far past the useful range.</summary>
public static class ListLimits
{
    public const int MaxOffset = 100_000;
    public const int MaxQueryLength = 200;
}

/// <summary>A parsed query or the text of the 400 response.</summary>
public sealed record ListQueryResult<T>(T? Query, string? Error)
    where T : class;

/// <summary>Turns the query string of the sites and host pages endpoints into read-model queries.</summary>
public static class ListQueries
{
    private const int MaxLimit = 200;

    public static ListQueryResult<SiteQuery> Sites(string? query, string? sort, int limit, int offset)
    {
        var parsed = ParseSort(sort);
        var error = parsed is null ? "Unknown sort." : Validate(query, offset);
        return error is not null
            ? new ListQueryResult<SiteQuery>(null, error)
            : new ListQueryResult<SiteQuery>(new SiteQuery(Clean(query), parsed!.Value, ClampLimit(limit), offset), null);
    }

    public static ListQueryResult<PageQuery> Pages(string? query, string? state, int limit, int offset)
    {
        var parsed = ParseState(state);
        var error = parsed is null ? "Unknown state." : Validate(query, offset);
        return error is not null
            ? new ListQueryResult<PageQuery>(null, error)
            : new ListQueryResult<PageQuery>(new PageQuery(Clean(query), parsed!.Value, ClampLimit(limit), offset), null);
    }

    private static string? Validate(string? query, int offset)
    {
        if (Clean(query)?.Length > ListLimits.MaxQueryLength)
        {
            return $"Query is longer than {ListLimits.MaxQueryLength} characters.";
        }

        return offset is < 0 or > ListLimits.MaxOffset
            ? $"Offset must be between 0 and {ListLimits.MaxOffset}."
            : null;
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
