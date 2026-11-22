using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IJobTreeReader
{
    Task<IReadOnlyList<FolderGroup>> Folders(int crawlJobId, string parent, CancellationToken ct);
}
