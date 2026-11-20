using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ICrawlJobRepository
{
    Task<CrawlJob?> Find(int id, CancellationToken ct);

    Task<IReadOnlyList<CrawlJob>> ListRecent(int limit, CancellationToken ct);

    Task<int> Insert(CrawlJob job, CancellationToken ct);

    Task Save(CrawlJob job, CancellationToken ct);

    Task<IReadOnlyList<int>> CompleteFinishedJobs(DateTimeOffset now, CancellationToken ct);
}
