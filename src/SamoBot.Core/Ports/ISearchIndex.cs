using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISearchIndex
{
    Task AddOrUpdate(IReadOnlyList<PageToIndex> pages, CancellationToken ct);
}
