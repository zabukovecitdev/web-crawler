using System.Text;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeSitemapSource : ISitemapSource
{
    private readonly Dictionary<string, byte[]> _bodies = [];
    private readonly Dictionary<string, int> _failures = [];

    public List<string> Downloaded { get; } = [];

    public FakeSitemapSource Serve(string url, string xml) => Serve(url, Encoding.UTF8.GetBytes(xml));

    public FakeSitemapSource Serve(string url, byte[] body)
    {
        _bodies[url] = body;
        return this;
    }

    public FakeSitemapSource Fail(string url, int statusCode)
    {
        _failures[url] = statusCode;
        return this;
    }

    public Task<SitemapDownload> Download(string url, CancellationToken ct)
    {
        Downloaded.Add(url);
        if (_failures.TryGetValue(url, out var status))
        {
            return Task.FromResult(new SitemapDownload(status, null));
        }

        return Task.FromResult(_bodies.TryGetValue(url, out var body) ? new SitemapDownload(200, body) : new SitemapDownload(404, null));
    }
}
