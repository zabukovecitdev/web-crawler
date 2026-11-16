using SamoBot.Core.Domain;
using static SamoBot.Core.Domain.LinkStepStatus;

namespace SamoBot.Core.Jobs;

/// <summary>
/// Explains one link of a job: which of the steps fetch, store, parse and index are done, and in words why the link is
/// where it is. The state itself comes from the query (<see cref="JobLinkFacts.State"/>), so the list, its filters and the
/// counts of a job always agree.
/// </summary>
public static class LinkStatusDescriber
{
    private sealed record Look(LinkSteps Steps, Func<JobLinkFacts, string> Reason);

    private static readonly LinkSteps StoppedAtFetch = new(Failed, Pending, Pending, Pending);

    private static readonly Dictionary<LinkState, Look> Looks = new()
    {
        [LinkState.Queued] = new(new(Waiting, Pending, Pending, Pending), _ => "Waiting in the queue to be fetched."),
        [LinkState.Fetching] = new(new(Active, Pending, Pending, Pending), _ => "Being fetched right now."),
        [LinkState.Retrying] = new(StoppedAtFetch, LinkReasons.Retrying),
        [LinkState.Failed] = new(StoppedAtFetch, LinkReasons.Failed),
        [LinkState.Skipped] = new(new(Done, Skipped, Pending, Pending), LinkReasons.Skipped),
        [LinkState.Parsing] = new(new(Done, Done, Waiting, Pending), _ => "Fetched and stored. Waiting for the parser."),
        [LinkState.Indexing] = new(new(Done, Done, Done, Waiting), _ => "Parsed. Waiting to be sent to the search index."),
        [LinkState.NotIndexed] = new(new(Done, Done, Done, Skipped), LinkReasons.NotIndexed),
        [LinkState.Indexed] = new(new(Done, Done, Done, Done), _ => "In the search index.")
    };

    private static readonly LinkSteps NeverRequested = new(Skipped, Pending, Pending, Pending);

    public static JobLink Describe(JobLinkFacts link)
    {
        var look = Looks[link.State];
        return new JobLink
        {
            Id = link.Id,
            Url = link.Url,
            Host = link.Host,
            Depth = link.Depth,
            Title = link.Title,
            State = link.State,
            Reason = look.Reason(link),
            Steps = WasRequested(link) ? look.Steps : NeverRequested,
            StatusCode = link.StatusCode,
            FailCount = link.FailCount,
            LastCrawlAt = link.LastCrawlAt,
            NextCrawlAt = link.NextCrawlAt,
            CanRecrawl = link.State != LinkState.Fetching
        };
    }

    /// <summary>A URL that robots.txt or the address policy stopped was never requested: its fetch step is skipped, not done.</summary>
    private static bool WasRequested(JobLinkFacts link) =>
        link.State != LinkState.Skipped || link.FailReason is not (LinkReasons.Robots or Blocked.Reason);
}
