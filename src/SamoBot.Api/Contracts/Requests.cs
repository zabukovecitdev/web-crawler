namespace SamoBot.Api.Contracts;

public sealed class CreateCrawlJobRequest
{
    public List<string> SeedUrls { get; set; } = [];
    public int? MaxDepth { get; set; }
    public int? MaxUrls { get; set; }
    public bool? UseJsRendering { get; set; }
    public bool? RespectRobots { get; set; }
    public bool? StartImmediately { get; set; }
}

public sealed class CrawlJobActionRequest
{
    public string Action { get; set; } = string.Empty;
}

public sealed class ExplainRequest
{
    public string Url { get; set; } = string.Empty;
}
