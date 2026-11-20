using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISiteReader
{
    Task<SitesPage> ListSites(SiteQuery query, CancellationToken ct);

    Task<SiteDetail?> FindSite(string host, CancellationToken ct);
}
