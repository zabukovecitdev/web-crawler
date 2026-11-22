namespace SamoBot.Api.Contracts;

public sealed class CreateCrawlJobRequest
{
    public List<string> SeedUrls { get; set; } = [];
    public int? MaxDepth { get; set; }
    public int? MaxUrls { get; set; }
    public bool? UseJsRendering { get; set; }
    public string? RenderMode { get; set; }
    public bool? JsNeeded { get; set; }
    public List<PathRuleRequest>? PathRules { get; set; }
    public bool? SeedFromSitemap { get; set; }
    public bool? FollowLinkedPages { get; set; }
    public int? ReviewAfterPages { get; set; }
    public bool? StartImmediately { get; set; }
}

public sealed class PathRuleRequest
{
    public string? Kind { get; set; }
    public string? Prefix { get; set; }
    public bool? Exact { get; set; }
}

public sealed class CrawlJobActionRequest
{
    public string Action { get; set; } = string.Empty;
}

public sealed class ExplainRequest
{
    public string Url { get; set; } = string.Empty;
}

public sealed class SiteCheckRequest
{
    public string Url { get; set; } = string.Empty;
}

public sealed class PathRulesRequest
{
    public List<PathRuleRequest> Rules { get; set; } = [];
}
