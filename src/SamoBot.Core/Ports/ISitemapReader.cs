using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISitemapReader
{
    Task<SitemapSnapshot> Read(string host, int limit, int offset, CancellationToken ct);
}
