using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IJobProgressReader
{
    Task<JobProgressData?> Read(int crawlJobId, CancellationToken ct);
}
