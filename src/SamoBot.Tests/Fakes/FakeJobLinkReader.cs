using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeJobLinkReader : IJobLinkReader
{
    public List<JobLinkFacts> Links { get; } = [];
    public Dictionary<int, JobLinkSummary> Summaries { get; } = [];
    public JobLinkQuery? LastQuery { get; private set; }

    public Task<PagedResult<JobLinkFacts>> List(int crawlJobId, JobLinkQuery query, CancellationToken ct)
    {
        LastQuery = query;
        return Task.FromResult(new PagedResult<JobLinkFacts>(Links.Count, Links));
    }

    public Task<IReadOnlyDictionary<int, JobLinkSummary>> Summarize(IReadOnlyList<int> crawlJobIds, CancellationToken ct) =>
        Task.FromResult<IReadOnlyDictionary<int, JobLinkSummary>>(Summaries);
}
