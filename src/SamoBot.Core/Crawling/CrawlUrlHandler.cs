using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

/// <summary>Crawls one claimed URL: robots check, fetch, store, classify, complete, publish.</summary>
public sealed class CrawlUrlHandler(
    RobotsGate robots,
    FetcherResolver fetchers,
    IContentStore contentStore,
    IUrlFrontier frontier,
    ICrawlEventPublisher events,
    TimeProvider timeProvider)
{
    public async Task Handle(ClaimedUrl url, CancellationToken ct)
    {
        if (!await robots.IsAllowed(url, ct))
        {
            await CompleteBlocked(url, ct);
            return;
        }

        await events.Publish(url.CrawlJobId, CrawlEventTypes.FetchStarted, new { url = url.Url, depth = url.Depth }, ct);
        var response = await fetchers.For(url).Fetch(url, ct);
        var objectName = await SaveContent(url, response, ct);
        var outcome = OutcomeClassifier.Classify(response, url.FailCount);
        await Complete(url, response, outcome, objectName, ct);
        await PublishFetchCompleted(url, response, outcome, ContentStatus.Of(response, objectName), ct);
    }

    private Task CompleteBlocked(ClaimedUrl url, CancellationToken ct)
    {
        var update = new Disabled("robots").ToUpdate(url, timeProvider.GetUtcNow());
        return frontier.Complete(url, update, null, ct);
    }

    private async Task<string?> SaveContent(ClaimedUrl url, FetchResponse response, CancellationToken ct)
    {
        if (!response.IsSuccess || !response.IsHtml || !response.HasContent)
        {
            return null;
        }

        return await contentStore.Save(url.Url, response.Content!, response.ContentType, ct);
    }

    private Task Complete(ClaimedUrl url, FetchResponse response, CrawlOutcome outcome, string? objectName, CancellationToken ct)
    {
        var now = timeProvider.GetUtcNow();
        var record = new FetchRecord
        {
            FetchedAt = now,
            StatusCode = response.StatusCode,
            ContentType = response.ContentType,
            ContentLength = response.Content?.Length,
            ResponseTimeMs = response.ResponseTimeMs,
            ObjectName = objectName,
            FinalUrl = url.Url
        };

        return frontier.Complete(url, outcome.ToUpdate(url, now), record, ct);
    }

    private Task PublishFetchCompleted(
        ClaimedUrl url, FetchResponse response, CrawlOutcome outcome, string contentStatus, CancellationToken ct)
    {
        var payload = new
        {
            url = url.Url,
            depth = url.Depth,
            statusCode = response.StatusCode,
            responseTimeMs = response.ResponseTimeMs,
            contentType = response.ContentType,
            contentLength = response.Content?.Length,
            contentStatus,
            delayMs = url.CrawlDelayMs,
            outcome = outcome.GetType().Name
        };
        return events.Publish(url.CrawlJobId, CrawlEventTypes.FetchCompleted, payload, ct);
    }
}
