using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeSitemapPlanReader(SitemapPlanStatus status) : ISitemapPlanReader
{
    public Task<SitemapPlanStatus> Status(string host, CancellationToken ct) => Task.FromResult(status);
}
