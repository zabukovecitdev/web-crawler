using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Downloads one sitemap file. An implementation never throws for an HTTP or network error, and caps the size of the body.</summary>
public interface ISitemapSource
{
    Task<SitemapDownload> Download(string url, CancellationToken ct);
}
