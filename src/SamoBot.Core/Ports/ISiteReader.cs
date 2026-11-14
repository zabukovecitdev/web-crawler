using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Read model of sites (hosts) for the dashboard.</summary>
public interface ISiteReader
{
    Task<SitesPage> ListSites(SiteQuery query, CancellationToken ct);

    Task<SiteDetail?> FindSite(string host, CancellationToken ct);
}
