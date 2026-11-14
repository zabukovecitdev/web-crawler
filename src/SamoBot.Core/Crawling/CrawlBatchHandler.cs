using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

public sealed class CrawlBatchHandler(IUrlFrontier frontier, CrawlUrlHandler urlHandler, InFlightHold hold) : IWorkHandler
{
    private const int BatchSize = 20;

    /// <summary>The URL lease. It is also the cap of the in-flight hold, so the hold never outlives the lease.</summary>
    public static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var urls = await frontier.Claim(BatchSize, Lease, hold.Duration, ct);
        await Task.WhenAll(urls.Select(url => urlHandler.Handle(url, ct)));
        return urls.Count;
    }
}
