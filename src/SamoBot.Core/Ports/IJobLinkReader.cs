using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IJobLinkReader
{
    Task<PagedResult<JobLinkFacts>> List(int crawlJobId, JobLinkQuery query, CancellationToken ct);

    Task<IReadOnlyDictionary<int, JobLinkSummary>> Summarize(IReadOnlyList<int> crawlJobIds, CancellationToken ct);
}
