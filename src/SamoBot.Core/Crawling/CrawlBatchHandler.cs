using SamoBot.Core.Browser;
using SamoBot.Core.Common;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

public sealed class CrawlBatchHandler(
    IUrlFrontier frontier,
    CrawlUrlHandler urlHandler,
    InFlightHold hold,
    BrowserAvailability browser) : IWorkHandler
{
    private const int BatchSize = 20;

    public static readonly TimeSpan Lease = TimeSpan.FromMinutes(5);

    public async Task<int> HandleBatch(CancellationToken ct)
    {
        var status = await browser.Current(ct);
        var urls = await frontier.Claim(BatchSize, Lease, hold.Duration, status.Available, ct);
        await Task.WhenAll(urls.Select(url => urlHandler.Handle(url, ct)));
        return urls.Count;
    }
}
