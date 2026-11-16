using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

/// <summary>Turns the query string of the job links endpoint into a read-model query.</summary>
public static class JobLinkQueries
{
    private const int MaxLimit = 200;
    private const int MaxQueryLength = 200;

    public static ListQueryResult<JobLinkQuery> Parse(string? filter, string? query, int limit, int offset)
    {
        if (!Enum.TryParse<JobLinkFilter>(filter ?? nameof(JobLinkFilter.All), ignoreCase: true, out var parsed) || !Enum.IsDefined(parsed))
        {
            return Invalid("Filter must be all, problems, notIndexed, pending or indexed.");
        }

        if (offset is < 0 or > ListLimits.MaxOffset)
        {
            return Invalid($"Offset must be between 0 and {ListLimits.MaxOffset}.");
        }

        return query is { Length: > MaxQueryLength }
            ? Invalid($"Query must be at most {MaxQueryLength} characters.")
            : new ListQueryResult<JobLinkQuery>(new JobLinkQuery(parsed, query, Math.Clamp(limit, 1, MaxLimit), offset), null);
    }

    private static ListQueryResult<JobLinkQuery> Invalid(string error) => new(null, error);
}
