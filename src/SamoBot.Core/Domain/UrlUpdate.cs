namespace SamoBot.Core.Domain;

/// <summary>The new state of a frontier URL after a crawl.</summary>
public sealed record UrlUpdate(
    UrlStatus Status,
    DateTimeOffset? NextCrawlAt,
    int FailCount,
    string? FailReason,
    NewUrl? NewUrl);
