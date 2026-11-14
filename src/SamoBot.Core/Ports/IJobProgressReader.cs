using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IJobProgressReader
{
    /// <summary>Reads the counters of a job in one query. Returns null when the job does not exist.</summary>
    Task<JobProgressData?> Read(int crawlJobId, CancellationToken ct);
}
