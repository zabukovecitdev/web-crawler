using SamoBot.Core.Domain;

namespace SamoBot.Core.Jobs;

/// <summary>
/// Explains one link of a job in words: why the link is where it is. The state itself comes from the query
/// (<see cref="JobLinkFacts.State"/>), so the list, its filters and the counts of a job always agree.
/// </summary>
public static class LinkStatusDescriber
{
    private static readonly Dictionary<LinkState, Func<JobLinkFacts, string>> Reasons = new()
    {
        [LinkState.Queued] = _ => "Waiting in the queue to be fetched.",
        [LinkState.Fetching] = _ => "Being fetched right now.",
        [LinkState.Retrying] = LinkReasons.Retrying,
        [LinkState.Failed] = LinkReasons.Failed,
        [LinkState.Skipped] = LinkReasons.Skipped,
        [LinkState.Parsing] = _ => "Fetched and stored. Waiting for the parser.",
        [LinkState.Indexing] = _ => "Parsed. Waiting to be sent to the search index.",
        [LinkState.NotIndexed] = LinkReasons.NotIndexed,
        [LinkState.Indexed] = _ => "In the search index."
    };

    public static JobLink Describe(JobLinkFacts link) => new()
    {
        Id = link.Id,
        Url = link.Url,
        Host = link.Host,
        Depth = link.Depth,
        Title = link.Title,
        State = link.State,
        Reason = Reasons[link.State](link),
        StatusCode = link.StatusCode,
        FailCount = link.FailCount,
        LastCrawlAt = link.LastCrawlAt,
        NextCrawlAt = link.NextCrawlAt,
        CanRecrawl = link.State != LinkState.Fetching
    };
}
