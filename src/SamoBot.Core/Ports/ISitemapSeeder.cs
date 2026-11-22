namespace SamoBot.Core.Ports;

public interface ISitemapSeeder
{
    Task<int> SeedFromSitemap(int crawlJobId, string host, CancellationToken ct);
}
