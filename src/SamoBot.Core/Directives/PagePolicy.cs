namespace SamoBot.Core.Directives;

public sealed record PagePolicy
{
    public static readonly PagePolicy Default = new();

    public bool FollowLinks { get; init; } = true;

    public bool Index { get; init; } = true;

    public DateTimeOffset? IndexUntil { get; init; }

    public int? SnippetMaxLength { get; init; }

    public bool Archive { get; init; } = true;

    public IReadOnlyList<string> Directives { get; init; } = [];

    public bool AllowsIndexingAt(DateTimeOffset now) => Index && (IndexUntil is null || IndexUntil > now);

    public PagePolicy WithSnippetLimit(int limit) =>
        this with { SnippetMaxLength = SnippetMaxLength is { } current ? Math.Min(current, limit) : limit };

    public PagePolicy WithIndexUntil(DateTimeOffset until) =>
        this with { IndexUntil = IndexUntil is { } current && current < until ? current : until };
}
