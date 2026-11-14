using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeSitemapReader(SitemapSnapshot snapshot) : ISitemapReader
{
    public Task<SitemapSnapshot> Read(string host, int limit, int offset, CancellationToken ct) => Task.FromResult(snapshot);
}
