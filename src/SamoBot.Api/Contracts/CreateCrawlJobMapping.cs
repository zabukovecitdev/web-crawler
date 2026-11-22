using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

public static class CreateCrawlJobMapping
{
    public const PathRuleKind UnknownKind = (PathRuleKind)(-1);

    public static NewCrawlJob ToNewJob(this CreateCrawlJobRequest request, string? owner) => new(
        owner,
        request.SeedUrls,
        request.MaxDepth,
        request.MaxUrls,
        RenderModes.FromRequest(request.RenderMode, request.UseJsRendering),
        request.JsNeeded ?? false,
        (request.PathRules ?? []).Select(ToRule).ToList(),
        request.SeedFromSitemap ?? false,
        request.FollowLinkedPages ?? true,
        request.ReviewAfterPages);

    public static PathRule ToRule(PathRuleRequest rule) => new(KindOf(rule.Kind), rule.Prefix ?? string.Empty, rule.Exact ?? false);

    private static PathRuleKind KindOf(string? kind) =>
        Enum.TryParse<PathRuleKind>(kind, ignoreCase: true, out var parsed) && Enum.IsDefined(parsed) ? parsed : UnknownKind;
}
