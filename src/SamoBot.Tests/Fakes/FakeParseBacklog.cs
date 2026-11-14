using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeParseBacklog(params FetchToParse[] due) : IParseBacklog
{
    public List<(FetchToParse Fetch, ParsedPage Page, IReadOnlyList<NewUrl> Links, IReadOnlyList<PageLink> PageLinks)> Completed { get; } = [];
    public (int BatchSize, TimeSpan Lease)? LastClaim { get; private set; }

    public Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct)
    {
        LastClaim = (batchSize, lease);
        return Task.FromResult<IReadOnlyList<FetchToParse>>(due);
    }

    public Task Complete(FetchToParse fetch, ParsedPage page, IReadOnlyList<NewUrl> links, IReadOnlyList<PageLink> pageLinks, CancellationToken ct)
    {
        Completed.Add((fetch, page, links, pageLinks));
        return Task.CompletedTask;
    }
}
