namespace SamoBot.Core.Domain;

/// <summary>The input that creates a crawl job.</summary>
public sealed record NewCrawlJob(
    string? OwnerUserId,
    IReadOnlyList<string> SeedUrls,
    int? MaxDepth,
    int? MaxUrls,
    bool UseJsRendering,
    bool RespectRobots);
