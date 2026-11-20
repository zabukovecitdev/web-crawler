using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IParseBacklog
{
    Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct);

    Task Complete(FetchToParse fetch, ParseResult result, CancellationToken ct);
}
