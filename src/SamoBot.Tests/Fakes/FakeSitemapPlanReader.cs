using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeSitemapPlanReader(SitemapPlanStatus status, IReadOnlyList<FolderGroup>? groups = null) : ISitemapPlanReader
{
    public string? LastParent { get; private set; }

    public Task<SitemapPlanStatus> Status(string host, CancellationToken ct) => Task.FromResult(status);

    public Task<IReadOnlyList<FolderGroup>> Folders(string host, string parent, CancellationToken ct)
    {
        LastParent = parent;
        return Task.FromResult(groups ?? []);
    }
}
