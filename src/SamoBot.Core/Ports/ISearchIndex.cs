using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISearchIndex
{
    Task AddOrUpdate(IReadOnlyList<PageToIndex> pages, CancellationToken ct);

    /// <summary>Removes the documents of these URLs (by discovered URL id). Ids that are not in the index are ignored.</summary>
    Task Remove(IReadOnlyList<int> discoveredUrlIds, CancellationToken ct);
}
