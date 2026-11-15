using SamoBot.Core.Directives;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed record CompletedParse(FetchToParse Fetch, ParsedPage Page, PagePolicy Policy, IReadOnlyList<NewUrl> Links, IReadOnlyList<PageLink> PageLinks);

public sealed class FakeParseBacklog(params FetchToParse[] due) : IParseBacklog
{
    public List<CompletedParse> Completed { get; } = [];
    public (int BatchSize, TimeSpan Lease)? LastClaim { get; private set; }

    public Task<IReadOnlyList<FetchToParse>> Claim(int batchSize, TimeSpan lease, CancellationToken ct)
    {
        LastClaim = (batchSize, lease);
        return Task.FromResult<IReadOnlyList<FetchToParse>>(due);
    }

    public Task Complete(FetchToParse fetch, ParseResult result, CancellationToken ct)
    {
        Completed.Add(new CompletedParse(fetch, result.Page, result.Policy, result.NewUrls, result.PageLinks));
        return Task.CompletedTask;
    }
}
