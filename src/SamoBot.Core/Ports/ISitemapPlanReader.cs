using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISitemapPlanReader
{
    Task<SitemapPlanStatus> Status(string host, CancellationToken ct);
}
