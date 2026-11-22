namespace SamoBot.Core.Domain;

public sealed record NewCrawlJob(
    string? OwnerUserId,
    IReadOnlyList<string> SeedUrls,
    int? MaxDepth,
    int? MaxUrls,
    RenderMode RenderMode,
    bool JsNeeded,
    IReadOnlyList<PathRule> PathRules,
    bool SeedFromSitemap,
    bool FollowLinkedPages,
    int? ReviewAfterPages);
