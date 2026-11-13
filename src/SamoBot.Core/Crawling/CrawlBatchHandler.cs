using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

public sealed class CrawlBatchHandler(IUrlFrontier frontier, CrawlUrlHandler urlHandler) : IWorkHandler
{
    private const int BatchSize = 20;
    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var urls = await frontier.Claim(BatchSize, Lease, ct);
        await Task.WhenAll(urls.Select(url => urlHandler.Handle(url, ct)));
        return urls.Count;
    }
}
