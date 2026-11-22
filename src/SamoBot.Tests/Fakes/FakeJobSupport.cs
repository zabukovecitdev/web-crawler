using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeJobTreeReader(IReadOnlyList<FolderGroup> groups) : IJobTreeReader
{
    public Task<IReadOnlyList<FolderGroup>> Folders(int crawlJobId, string parent, CancellationToken ct) => Task.FromResult(groups);
}

public sealed class FakeSitemapSeeder(int seeded = 0) : ISitemapSeeder
{
    public List<(int JobId, string Host)> Seeded { get; } = [];

    public Task<int> SeedFromSitemap(int crawlJobId, string host, CancellationToken ct)
    {
        Seeded.Add((crawlJobId, host));
        return Task.FromResult(seeded);
    }
}

public sealed class FakePathRuleApplier(PathRuleEffect effect) : IPathRuleApplier
{
    public List<int> Applied { get; } = [];

    public Task<PathRuleEffect> Apply(int crawlJobId, CancellationToken ct)
    {
        Applied.Add(crawlJobId);
        return Task.FromResult(effect);
    }
}
