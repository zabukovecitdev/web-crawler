using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>The dashboard search: paged results with a snippet and the match count per host.</summary>
public interface ISearchPages
{
    Task<SearchPage> Search(SearchRequest request, CancellationToken ct);
}
