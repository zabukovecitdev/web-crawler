namespace SamoBot.Core.Domain;

public sealed record SearchResult(int PageId, string Host, string Url, string Title, string Snippet, string Language, DateTimeOffset? FetchedAt);

public sealed record HostCount(string Host, int Count);

public sealed record SearchPage(int Total, long TookMs, IReadOnlyList<HostCount> Hosts, IReadOnlyList<SearchResult> Items);
