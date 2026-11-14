using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Read model of the stored sitemaps of a host. A host without sitemaps gives an empty snapshot.</summary>
public interface ISitemapReader
{
    Task<SitemapSnapshot> Read(string host, int limit, int offset, CancellationToken ct);
}
