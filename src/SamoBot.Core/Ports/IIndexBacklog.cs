using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IIndexBacklog
{
    Task<IReadOnlyList<PageToIndex>> Claim(int batchSize, CancellationToken ct);

    Task MarkIndexed(IReadOnlyList<int> pageIds, CancellationToken ct);
}
