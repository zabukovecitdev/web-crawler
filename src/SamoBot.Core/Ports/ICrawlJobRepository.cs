using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ICrawlJobRepository
{
    Task<CrawlJob?> Get(int id, CancellationToken ct);

    Task<IReadOnlyList<CrawlJob>> ListRecent(int limit, CancellationToken ct);

    /// <summary>Inserts the job and returns the new id.</summary>
    Task<int> Insert(CrawlJob job, CancellationToken ct);

    Task Save(CrawlJob job, CancellationToken ct);

    /// <summary>Completes each running job that has no work left. Returns the ids of the completed jobs.</summary>
    Task<IReadOnlyList<int>> CompleteFinishedJobs(DateTimeOffset now, CancellationToken ct);
}
