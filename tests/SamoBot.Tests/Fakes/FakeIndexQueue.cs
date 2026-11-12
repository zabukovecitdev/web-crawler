using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeIndexQueue(params PageToIndex[] due) : IIndexQueue
{
    public int? LastBatchSize { get; private set; }
    public List<int> Indexed { get; } = [];

    public Task<IReadOnlyList<PageToIndex>> Claim(int batchSize, CancellationToken ct)
    {
        LastBatchSize = batchSize;
        return Task.FromResult<IReadOnlyList<PageToIndex>>(due);
    }

    public Task MarkIndexed(IReadOnlyList<int> pageIds, CancellationToken ct)
    {
        Indexed.AddRange(pageIds);
        return Task.CompletedTask;
    }
}
