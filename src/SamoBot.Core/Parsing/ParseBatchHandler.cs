using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Parsing;

public sealed class ParseBatchHandler(IParseBacklog queue, ParsePageHandler pageHandler) : IWorkHandler
{
    private const int BatchSize = 10;
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var fetches = await queue.Claim(BatchSize, Lease, ct);
        await Task.WhenAll(fetches.Select(fetch => pageHandler.Handle(fetch, ct)));
        return fetches.Count;
    }
}
