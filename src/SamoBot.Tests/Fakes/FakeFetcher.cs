using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Tests.Fakes;

public sealed class FakeFetcher(bool supports = true, FetchResponse? response = null) : IFetcher
{
    public List<ClaimedUrl> Fetched { get; } = [];

    public bool Supports(ClaimedUrl url) => supports;

    public Task<FetchResponse> Fetch(ClaimedUrl url, CancellationToken ct)
    {
        Fetched.Add(url);
        return Task.FromResult(response ?? TestData.Response(200));
    }
}
