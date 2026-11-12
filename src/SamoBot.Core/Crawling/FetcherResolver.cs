using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

public sealed class FetcherResolver(IEnumerable<IFetcher> fetchers)
{
    private readonly IReadOnlyList<IFetcher> _fetchers = fetchers.ToList();

    public IFetcher For(ClaimedUrl url) =>
        _fetchers.FirstOrDefault(fetcher => fetcher.Supports(url))
        ?? throw new InvalidOperationException($"No fetcher supports the URL {url.Url}.");
}
