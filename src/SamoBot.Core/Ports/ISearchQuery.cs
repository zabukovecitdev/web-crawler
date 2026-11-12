using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISearchQuery
{
    Task<IReadOnlyList<SearchHit>> Search(string text, int limit, CancellationToken ct);
}
