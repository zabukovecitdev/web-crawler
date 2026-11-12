using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeParseQueue(params FetchToParse[] due) : IParseQueue
{
    public List<(FetchToParse Fetch, ParsedPage Page, IReadOnlyList<NewUrl> Links)> Completed { get; } = [];
    public (int BatchSize, TimeSpan Lease)? LastClaim { get; private set; }

    public Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct)
    {
        LastClaim = (batchSize, lease);
        return Task.FromResult<IReadOnlyList<FetchToParse>>(due);
    }

    public Task Complete(FetchToParse fetch, ParsedPage page, IReadOnlyList<NewUrl> links, CancellationToken ct)
    {
        Completed.Add((fetch, page, links));
        return Task.CompletedTask;
    }
}
