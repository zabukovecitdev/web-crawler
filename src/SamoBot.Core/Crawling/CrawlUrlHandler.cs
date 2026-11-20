using SamoBot.Core.Common;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

public sealed class CrawlUrlHandler(
    RobotsGate robots,
    FetcherResolver fetchers,
    FetchRecorder recorder,
    ICrawlEventPublisher events)
{
    public async Task Handle(ClaimedUrl url, CancellationToken ct)
    {
        try
        {
            if (!await robots.IsAllowed(url, ct))
            {
                await recorder.RecordDisallowed(url, ct);
                return;
            }
        }
        catch (BlockedAddressException)
        {
            await Finish(url, FetchResponse.BlockedAddress(), ct);
            return;
        }

        await events.Publish(url.CrawlJobId, CrawlEventTypes.FetchStarted, new { url = url.Url, depth = url.Depth }, ct);
        await Finish(url, await fetchers.For(url).Fetch(url, ct), ct);
    }

    private async Task Finish(ClaimedUrl url, FetchResponse response, CancellationToken ct)
    {
        var outcome = OutcomeClassifier.Classify(response, url.FailCount);
        var recorded = await recorder.Record(url, response, outcome, ct);
        await PublishFetchCompleted(url, response, outcome, recorded, ct);
    }

    private Task PublishFetchCompleted(
        ClaimedUrl url, FetchResponse response, CrawlOutcome outcome, RecordedFetch recorded, CancellationToken ct)
    {
        var payload = new
        {
            pageId = url.Id,
            host = url.Host,
            url = url.Url,
            depth = url.Depth,
            statusCode = response.StatusCode,
            responseTimeMs = response.ResponseTimeMs,
            contentType = response.ContentType,
            contentLength = response.Content?.Length,
            contentStatus = ContentStatus.Of(response, recorded.ObjectName),
            delayMs = (int)recorded.Delay.TotalMilliseconds,
            outcome = outcome.GetType().Name
        };
        return events.Publish(url.CrawlJobId, CrawlEventTypes.FetchCompleted, payload, ct);
    }
}
