using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Read model of the links of crawl jobs: where each link stands on its way to the search index.</summary>
public interface IJobLinkReader
{
    Task<PagedResult<JobLinkFacts>> List(int crawlJobId, JobLinkQuery query, CancellationToken ct);

    /// <summary>The link counts of each of the jobs. A job without links has no entry.</summary>
    Task<IReadOnlyDictionary<int, JobLinkSummary>> Summarize(IReadOnlyList<int> crawlJobIds, CancellationToken ct);
}
