namespace SamoBot.Core.Domain;

public sealed record NewCrawlJob(
    string? OwnerUserId,
    IReadOnlyList<string> SeedUrls,
    int? MaxDepth,
    int? MaxUrls,
    bool UseJsRendering);
