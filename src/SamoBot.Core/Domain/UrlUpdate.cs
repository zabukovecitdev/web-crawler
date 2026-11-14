namespace SamoBot.Core.Domain;

/// <summary>The new state of a frontier URL after a crawl. <see cref="Pacing"/> is the new pacing of its host (null: unchanged).</summary>
public sealed record UrlUpdate(
    UrlStatus Status,
    DateTimeOffset? NextCrawlAt,
    int FailCount,
    string? FailReason,
    NewUrl? NewUrl,
    HostPacingUpdate? Pacing = null);
