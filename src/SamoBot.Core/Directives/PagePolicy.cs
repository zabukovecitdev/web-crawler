namespace SamoBot.Core.Directives;

/// <summary>
/// What the crawler may do with one page, decided from what the site said (see <see cref="PagePolicyEvaluator"/>).
/// The defaults allow everything; each rule can only make the policy stricter.
/// </summary>
public sealed record PagePolicy
{
    public static readonly PagePolicy Default = new();

    /// <summary>False: none of the links of the page are queued (nofollow, none).</summary>
    public bool FollowLinks { get; init; } = true;

    /// <summary>False: the page is kept out of the search index (noindex, none, a canonical that names another page).</summary>
    public bool Index { get; init; } = true;

    /// <summary>The page leaves the search index at this time (unavailable_after). Null: no end.</summary>
    public DateTimeOffset? IndexUntil { get; init; }

    /// <summary>The longest search snippet in characters. Null: no limit; 0: no snippet (nosnippet, max-snippet).</summary>
    public int? SnippetMaxLength { get; init; }

    /// <summary>False: the stored copy of the page is not shown (noarchive).</summary>
    public bool Archive { get; init; } = true;

    /// <summary>The directives that applied to this crawler, with their source, for display.</summary>
    public IReadOnlyList<string> Directives { get; init; } = [];

    /// <summary>True when the page may be in the search index at <paramref name="now"/>.</summary>
    public bool AllowsIndexingAt(DateTimeOffset now) => Index && (IndexUntil is null || IndexUntil > now);

    /// <summary>The stricter of the current snippet limit and <paramref name="limit"/>.</summary>
    public PagePolicy WithSnippetLimit(int limit) =>
        this with { SnippetMaxLength = SnippetMaxLength is { } current ? Math.Min(current, limit) : limit };

    /// <summary>The earlier of the current end of indexing and <paramref name="until"/>.</summary>
    public PagePolicy WithIndexUntil(DateTimeOffset until) =>
        this with { IndexUntil = IndexUntil is { } current && current < until ? current : until };
}
