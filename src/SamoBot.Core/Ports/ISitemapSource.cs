using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISitemapSource
{
    Task<SitemapDownload> Download(string url, CancellationToken ct);
}
