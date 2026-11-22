namespace SamoBot.Adapters.Postgres;

internal static class FolderGroupSql
{
    public static string Group(string rowsSql, string aggregates) => $"""
        WITH rows AS ({rowsSql}),
        under AS (SELECT r.*, substr(r.path, length(@Parent) + 1) AS rest FROM rows r WHERE starts_with(r.path, @Parent)),
        first AS (SELECT u.*, substring(u.rest from '^[^/]*/?') AS first FROM under u),
        second AS (
            SELECT f.*, CASE WHEN right(f.first, 1) = '/' THEN substring(substr(f.rest, length(f.first) + 1) from '^[^/]*/?') ELSE '' END AS second
            FROM first f)
        SELECT first AS "First", second AS "Second", {aggregates}
        FROM second GROUP BY first, second
        """;
}

internal sealed record FolderRow
{
    public string First { get; init; } = string.Empty;
    public string Second { get; init; } = string.Empty;
    public int Pages { get; init; }
    public int Done { get; init; }
    public int Failed { get; init; }
    public int Skipped { get; init; }
    public int Stopped { get; init; }
    public int Fetching { get; init; }
    public int Queued { get; init; }
    public DateTimeOffset? OldestModified { get; init; }
    public DateTimeOffset? NewestModified { get; init; }

    public SamoBot.Core.Domain.FolderGroup ToGroup() => new(
        First,
        Second,
        new SamoBot.Core.Domain.FolderCounts(Pages, Done, Failed, Skipped, Stopped, Fetching, Queued, OldestModified, NewestModified));
}
