namespace SamoBot.Core.Domain;

public sealed record UrlUpdate(
    UrlStatus Status,
    DateTimeOffset? NextCrawlAt,
    int FailCount,
    string? FailReason,
    NewUrl? NewUrl,
    HostPacingUpdate? Pacing = null);
