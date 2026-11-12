using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IParseQueue
{
    Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct);

    /// <summary>Stores the parsed page and the new links, and marks the fetch as parsed, in one transaction.</summary>
    Task Complete(FetchToParse fetch, ParsedPage page, IReadOnlyList<NewUrl> links, CancellationToken ct);
}
