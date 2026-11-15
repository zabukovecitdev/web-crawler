using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Core.Crawling;

/// <summary>What <see cref="FetchRecorder"/> stored: the object name of the saved body, and the delay applied to the host.</summary>
public sealed record RecordedFetch(string? ObjectName, TimeSpan Delay);

/// <summary>
/// Stores the body of a fetch and records the result in the frontier: the fetch row, the new URL state and the pacing of
/// the host, all in one transaction (<see cref="IUrlFrontier.Complete"/>).
/// </summary>
public sealed class FetchRecorder(
    IContentStore contentStore,
    IUrlFrontier frontier,
    PolitenessPolicy politeness,
    TimeProvider timeProvider)
{
    public async Task<RecordedFetch> Record(ClaimedUrl url, FetchResponse response, CrawlOutcome outcome, CancellationToken ct)
    {
        var objectName = await SaveContent(url, response, ct);
        var now = timeProvider.GetUtcNow();
        var pacing = politeness.Next(url, response);
        var update = outcome.ToUpdate(url, now) with { Pacing = pacing };
        await frontier.Complete(url, update, ToRecord(url, response, objectName, now), ct);
        return new RecordedFetch(objectName, pacing?.Delay ?? TimeSpan.FromMilliseconds(url.CrawlDelayMs));
    }

    /// <summary>A URL that robots.txt disallows. No request was made, so the host keeps its pacing state.</summary>
    public Task RecordDisallowed(ClaimedUrl url, CancellationToken ct)
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

    private static FetchRecord ToRecord(ClaimedUrl url, FetchResponse response, string? objectName, DateTimeOffset now) => new()
    {
        FetchedAt = now,
        StatusCode = response.StatusCode,
        ContentType = response.ContentType,
        ContentLength = response.Content?.Length,
        ResponseTimeMs = response.ResponseTimeMs,
        ObjectName = objectName,
        FinalUrl = url.Url,
        ResponseHeaders = response.ResponseHeaders
    };
}
