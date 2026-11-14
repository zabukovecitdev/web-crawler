using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IParseBacklog
{
    Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct);

    /// <summary>
    /// Stores the parsed page, adds the new URLs to the frontier, replaces the stored links of the page
    /// and marks the fetch as parsed, in one transaction.
    /// </summary>
    Task Complete(FetchToParse fetch, ParsedPage page, IReadOnlyList<NewUrl> links, IReadOnlyList<PageLink> pageLinks, CancellationToken ct);
}
