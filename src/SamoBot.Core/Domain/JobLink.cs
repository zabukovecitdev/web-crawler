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

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum LinkStepStatus
{
    /// <summary>The step was not reached.</summary>
    Pending,
    Waiting,
    Active,
    Done,
    Skipped,
    Failed
}

/// <summary>The four steps every link goes through, in order.</summary>
public sealed record LinkSteps(LinkStepStatus Fetch, LinkStepStatus Store, LinkStepStatus Parse, LinkStepStatus Index);

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

/// <summary>One link of a job as the dashboard shows it: its state, the reason in words and the state of each step.</summary>
public sealed record JobLink
{
    public int Id { get; init; }
    public string Url { get; init; } = string.Empty;
    public string Host { get; init; } = string.Empty;
    public int Depth { get; init; }
    public string? Title { get; init; }
    public LinkState State { get; init; }
    public string Reason { get; init; } = string.Empty;
    public required LinkSteps Steps { get; init; }
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
public sealed record JobLinkSummary(IReadOnlyDictionary<LinkState, int> Counts, IReadOnlyList<LinkProblem> Problems)
{
    public static readonly JobLinkSummary Empty = new(new Dictionary<LinkState, int>(), []);

    public int Total => Counts.Values.Sum();
    public int Queued => Of(LinkState.Queued);
    public int Fetching => Of(LinkState.Fetching);
    public int Retrying => Of(LinkState.Retrying);
    public int Parsing => Of(LinkState.Parsing);
    public int Indexing => Of(LinkState.Indexing);
    public int Indexed => Of(LinkState.Indexed);
    public int NotIndexed => Of(LinkState.NotIndexed);
    public int Skipped => Of(LinkState.Skipped);
    public int Failed => Of(LinkState.Failed);

    [JsonIgnore]
    public IReadOnlyDictionary<LinkState, int> Counts { get; } = Counts;

    private int Of(LinkState state) => Counts.GetValueOrDefault(state);
}

/// <summary>Which links a recrawl puts back in the queue: one link, the failed links, or every link of the job.</summary>
public sealed record RequeueScope(int? UrlId, bool FailedOnly)
{
    public static readonly RequeueScope All = new(null, false);
    public static readonly RequeueScope Failed = new(null, true);

    public static RequeueScope Link(int urlId) => new(urlId, false);
}
