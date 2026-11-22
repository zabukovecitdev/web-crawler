using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISitemapPlanReader
{
    Task<SitemapPlanStatus> Status(string host, CancellationToken ct);

    Task<IReadOnlyList<FolderGroup>> Folders(string host, string parent, CancellationToken ct);
}
