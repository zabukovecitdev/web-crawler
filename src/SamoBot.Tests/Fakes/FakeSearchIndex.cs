using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeSearchIndex : ISearchIndex
{
    public List<IReadOnlyList<PageToIndex>> Calls { get; } = [];

    public Task AddOrUpdate(IReadOnlyList<PageToIndex> pages, CancellationToken ct)
    {
        Calls.Add(pages);
        return Task.CompletedTask;
    }
}
