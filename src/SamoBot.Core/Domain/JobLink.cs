using System.Text.Json.Serialization;
using SamoBot.Core.Directives;

namespace SamoBot.Core.Domain;

/// <summary>Where a link of a crawl job stands on its way to the search index. The dashboard reads these names.</summary>
[JsonConverter(typeof(JsonStringEnumConverter))]
public enum LinkState
{
    Queued,
    Fetching,
    Retrying,
    Parsing,
    Indexing,
    Indexed,
    NotIndexed,
    Skipped,
    Failed
}

/// <summary>What the database knows about one link of a job. <see cref="State"/> is derived by the query.</summary>
public sealed record JobLinkFacts
{
    public int Id { get; init; }
    public string Url { get; init; } = string.Empty;
    public string Host { get; init; } = string.Empty;
    public int Depth { get; init; }
    public LinkState State { get; init; }
    public string? FailReason { get; init; }
    public int FailCount { get; init; }
    public DateTimeOffset? LastCrawlAt { get; init; }
    public DateTimeOffset? NextCrawlAt { get; init; }
    public int? StatusCode { get; init; }
    public string? ContentType { get; init; }
    public long? ContentLength { get; init; }
    public string? Title { get; init; }
    public PagePolicy? Policy { get; init; }
}

/// <summary>One link of a job as the dashboard shows it: its state and the reason in words.</summary>
public sealed record JobLink
{
    public int Id { get; init; }
    public required string Url { get; init; }
    public required string Host { get; init; }
    public int Depth { get; init; }
    public string? Title { get; init; }
    public LinkState State { get; init; }
    public required string Reason { get; init; }
    public int? StatusCode { get; init; }
    public int FailCount { get; init; }
    public DateTimeOffset? LastCrawlAt { get; init; }
    public DateTimeOffset? NextCrawlAt { get; init; }
    public bool CanRecrawl { get; init; }
}

public enum JobLinkFilter
{
    All,
    Problems,
    NotIndexed,
    Pending,
    Indexed
}

public sealed record JobLinkQuery(JobLinkFilter Filter, string? Query, int Limit, int Offset);

/// <summary>A reason that links of a job failed, with the number of links.</summary>
public sealed record LinkProblem(string Reason, string Label, int Count);

/// <summary>The number of links of a job in each state, and why the failed ones failed.</summary>
public sealed record JobLinkSummary(
    int Total,
    int Queued,
    int Fetching,
    int Retrying,
    int Parsing,
    int Indexing,
    int Indexed,
    int NotIndexed,
    int Skipped,
    int Failed,
    IReadOnlyList<LinkProblem> Problems)
{
    public static readonly JobLinkSummary Empty = new(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, []);
}

/// <summary>Which links a recrawl puts back in the queue: one link, the failed links, or every link of the job.</summary>
public sealed record RequeueScope(int? UrlId, bool FailedOnly)
{
    public static readonly RequeueScope All = new(null, false);
    public static readonly RequeueScope Failed = new(null, true);

    public static RequeueScope Link(int urlId) => new(urlId, false);
}
