namespace SamoBot.Core.Domain;

public sealed record PageDetail : PageSummary
{
    public string Host { get; init; } = string.Empty;
    public int? CrawlJobId { get; init; }
    public DateTimeOffset? NextCrawlAt { get; init; }
    public string? FailReason { get; init; }
    public bool UsedJsRendering { get; init; }
    public IReadOnlyList<PageFetch> Fetches { get; init; } = [];
    public PageParsed? Parsed { get; init; }
}

public sealed record PageFetch
{
    public DateTimeOffset FetchedAt { get; init; }
    public int StatusCode { get; init; }
    public string? ContentType { get; init; }
    public long? ContentLength { get; init; }
    public long? ResponseTimeMs { get; init; }
    public string? FinalUrl { get; init; }
}

public sealed record PageHeading(int Level, string Text);

public sealed record PageParsed
{
    public string Title { get; init; } = string.Empty;
    public string Description { get; init; } = string.Empty;
    public string Language { get; init; } = string.Empty;
    public string Canonical { get; init; } = string.Empty;
    public string RobotsDirectives { get; init; } = string.Empty;
    public string BodyText { get; init; } = string.Empty;
    public int WordCount { get; init; }
    public IReadOnlyList<PageHeading> Headings { get; init; } = [];
    public int LinkCount { get; init; }
    public int InternalLinkCount { get; init; }
    public int ImageCount { get; init; }
    public int JsonLdBlocks { get; init; }
    public IReadOnlyDictionary<string, string> OpenGraph { get; init; } = new Dictionary<string, string>();
}
