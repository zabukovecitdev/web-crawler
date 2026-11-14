using SamoBot.Core.Domain;

namespace SamoBot.Adapters.Postgres;

/// <summary>One page query row. The parsed columns are only set by the detail query.</summary>
internal sealed class PageRow
{
    public int Id { get; init; }
    public string Url { get; init; } = string.Empty;
    public string Host { get; init; } = string.Empty;
    public int Depth { get; init; }
    public DateTimeOffset? LastCrawlAt { get; init; }
    public DateTimeOffset? NextCrawlAt { get; init; }
    public string? FailReason { get; init; }
    public int? CrawlJobId { get; init; }
    public bool UseJsRendering { get; init; }
    public string? Title { get; init; }
    public int? StatusCode { get; init; }
    public string? ContentType { get; init; }
    public long? ContentLength { get; init; }
    public long? ResponseTimeMs { get; init; }
    public string? ObjectName { get; init; }
    public bool HasParsedDocument { get; init; }
    public bool Indexed { get; init; }
    public string? Description { get; init; }
    public string? Language { get; init; }
    public string? Canonical { get; init; }
    public string? RobotsDirectives { get; init; }
    public string? BodyText { get; init; }
    public string? Headings { get; init; }
    public string? Images { get; init; }
    public string? OpenGraphData { get; init; }
    public string? JsonLdData { get; init; }

    public PageSummary ToSummary() => new()
    {
        Id = Id,
        Url = Url,
        Path = PathOf(Url),
        Title = Title,
        StatusCode = StatusCode,
        ContentType = ContentType,
        ContentLength = ContentLength,
        ResponseTimeMs = ResponseTimeMs,
        Depth = Depth,
        LastCrawlAt = LastCrawlAt,
        ContentState = ContentStates.Derive(Facts())
    };

    public PageDetail ToDetail(IReadOnlyList<PageFetch> fetches) => new()
    {
        Id = Id,
        Url = Url,
        Path = PathOf(Url),
        Title = Title,
        StatusCode = StatusCode,
        ContentType = ContentType,
        ContentLength = ContentLength,
        ResponseTimeMs = ResponseTimeMs,
        Depth = Depth,
        LastCrawlAt = LastCrawlAt,
        ContentState = ContentStates.Derive(Facts()),
        Host = Host,
        CrawlJobId = CrawlJobId,
        NextCrawlAt = NextCrawlAt,
        FailReason = FailReason,
        UsedJsRendering = UseJsRendering,
        Fetches = fetches,
        Parsed = HasParsedDocument ? ParsedPageMapper.Map(this) : null
    };

    private PageStateFacts Facts() => new()
    {
        StatusCode = StatusCode,
        ContentType = ContentType,
        ObjectName = ObjectName,
        HasParsedDocument = HasParsedDocument,
        Indexed = Indexed,
        FailReason = FailReason
    };

    private static string PathOf(string url) =>
        Uri.TryCreate(url, UriKind.Absolute, out var uri) ? uri.PathAndQuery : url;
}
