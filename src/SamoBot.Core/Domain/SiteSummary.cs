using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum SiteStatus
{
    Crawling,
    Crawled,
    Paused,
    Never
}

/// <summary>A site is a host. The row aggregates every job that touched the host.</summary>
public record SiteSummary
{
    public string Host { get; init; } = string.Empty;
    public SiteStatus Status { get; init; }
    public int Pages { get; init; }
    public int Failed { get; init; }
    public double? AvgResponseMs { get; init; }
    public int CrawlDelayMs { get; init; }
    public DateTimeOffset? LastCrawlAt { get; init; }
    public int? ActiveJobId { get; init; }
}

public sealed record SiteDetail : SiteSummary
{
    public DateTimeOffset? FirstCrawlAt { get; init; }
    public IReadOnlyList<StatusCount> StatusCodes { get; init; } = [];
    public IReadOnlyList<ContentTypeCount> ContentTypes { get; init; } = [];
}

public sealed record StatusCount
{
    public int StatusCode { get; init; }
    public int Count { get; init; }
}

public sealed record ContentTypeCount
{
    public string ContentType { get; init; } = string.Empty;
    public int Count { get; init; }
}

public sealed record SitesOverview
{
    public int Sites { get; init; }
    public int Pages { get; init; }
    public int Failed { get; init; }
    public int Crawling { get; init; }
    public double? MedianResponseMs { get; init; }
    public double? P95ResponseMs { get; init; }
}

public sealed record SitesPage(int Total, IReadOnlyList<SiteSummary> Items, SitesOverview Overview);

public enum SiteSort
{
    Host,
    Pages,
    Failed,
    LastCrawl
}

public sealed record SiteQuery(string? Query, SiteSort Sort, int Limit, int Offset);
