using SamoBot.Infrastructure.Models;

namespace SamoBot.Infrastructure.Graph.Abstractions;

public interface IPageGraphRepository
{
    Task UpsertPageLinksAsync(
        string sourceUrl,
        string normalizedSourceUrl,
        string sourceHost,
        int discoveredUrlId,
        int parsedDocumentId,
        IReadOnlyList<ParsedLink> links,
        CancellationToken cancellationToken = default);
}
