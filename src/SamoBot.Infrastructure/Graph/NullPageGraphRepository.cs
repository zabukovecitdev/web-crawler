using SamoBot.Infrastructure.Graph.Abstractions;
using SamoBot.Infrastructure.Models;

namespace SamoBot.Infrastructure.Graph;

public class NullPageGraphRepository : IPageGraphRepository
{
    public Task UpsertPageLinksAsync(
        string sourceUrl,
        string normalizedSourceUrl,
        string sourceHost,
        int discoveredUrlId,
        int parsedDocumentId,
        IReadOnlyList<ParsedLink> links,
        CancellationToken cancellationToken = default)
        => Task.CompletedTask;
}
