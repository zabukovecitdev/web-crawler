using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface ISearchPages
{
    Task<SearchPage> Search(SearchRequest request, CancellationToken ct);
}
