using System.Text;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

/// <summary>Serves sitemap bodies by URL. An unknown URL gives 404.</summary>
public sealed class FakeSitemapSource : ISitemapSource
{
    private readonly Dictionary<string, byte[]> _bodies = [];

    public List<string> Downloaded { get; } = [];

    public FakeSitemapSource Serve(string url, string xml) => Serve(url, Encoding.UTF8.GetBytes(xml));

    public FakeSitemapSource Serve(string url, byte[] body)
    {
        _bodies[url] = body;
        return this;
    }

    public Task<SitemapDownload> Download(string url, CancellationToken ct)
    {
        Downloaded.Add(url);
        return Task.FromResult(_bodies.TryGetValue(url, out var body) ? new SitemapDownload(200, body) : new SitemapDownload(404, null));
    }
}
