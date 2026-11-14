namespace SamoBot.Core.Domain;

/// <summary>One result of a dashboard search. Snippet is encoded text with only mark tags, see <c>SnippetFormatter</c>.</summary>
public sealed record SearchResult(int PageId, string Host, string Url, string Title, string Snippet, string Language, DateTimeOffset? FetchedAt);

public sealed record HostCount(string Host, int Count);

/// <summary>A page of results, the number of matches, the time the engine took and the matches per host.</summary>
public sealed record SearchPage(int Total, long TookMs, IReadOnlyList<HostCount> Hosts, IReadOnlyList<SearchResult> Items);
