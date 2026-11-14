using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

/// <summary>Turns the query string of the page links endpoint into a read-model query.</summary>
public static class LinkListQueries
{
    private const int MaxLimit = 200;

    public static ListQueryResult<LinkQuery> Parse(string? direction, int limit, int offset)
    {
        var parsed = ParseDirection(direction);
        if (parsed is null)
        {
            return new ListQueryResult<LinkQuery>(null, "Direction must be out or in.");
        }

        return offset is < 0 or > ListLimits.MaxOffset
            ? new ListQueryResult<LinkQuery>(null, $"Offset must be between 0 and {ListLimits.MaxOffset}.")
            : new ListQueryResult<LinkQuery>(new LinkQuery(parsed.Value, Math.Clamp(limit, 1, MaxLimit), offset), null);
    }

    private static LinkDirection? ParseDirection(string? direction) => direction?.ToLowerInvariant() switch
    {
        "out" => LinkDirection.Out,
        "in" => LinkDirection.In,
        _ => null
    };
}
