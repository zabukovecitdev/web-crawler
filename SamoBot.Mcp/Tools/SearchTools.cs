using System.ComponentModel;
using ModelContextProtocol.Server;
using SamoBot.Core.Ports;

namespace SamoBot.Mcp.Tools;

internal sealed class SearchTools(ISearchQuery search)
{
    [McpServerTool]
    [Description("Searches indexed pages and returns the title and canonical URL of each match.")]
    public async Task<IReadOnlyList<PageMatch>> SearchPages(
        [Description("The search query")]
        string query,
        [Description("Maximum number of results to return (default 20)")]
        int limit = 20,
        [Description("Number of results to skip for pagination (default 0)")]
        int offset = 0,
        CancellationToken cancellationToken = default)
    {
        var hits = await search.Search(query, limit + offset, cancellationToken);
        return hits.Skip(offset).Select(hit => new PageMatch(hit.Title, hit.Canonical)).ToList();
    }
}

internal sealed record PageMatch(string Title, string Canonical);
