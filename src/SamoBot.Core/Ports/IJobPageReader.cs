using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IJobPageReader
{
    Task<IReadOnlyList<JobPage>> ListPages(int crawlJobId, int limit, int offset, CancellationToken ct);

    Task<int> CountPages(int crawlJobId, CancellationToken ct);

    Task<IReadOnlyList<JobEvent>> ListEvents(int crawlJobId, long afterId, int limit, CancellationToken ct);
}
