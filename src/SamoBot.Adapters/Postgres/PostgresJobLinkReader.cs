using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Jobs;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// Reads the links of crawl jobs with their state. The state is one SQL expression (<see cref="StateExpression"/>), used by the
/// list, its filters and the counts, so that they cannot disagree.
/// </summary>
public sealed class PostgresJobLinkReader(NpgsqlDataSource dataSource, TimeProvider timeProvider) : IJobLinkReader
{
    // The order matters: the frontier state comes first (in flight, waiting for a retry, queued, stopped for good),
    // then the last fetch, then the parsed document and its policy.
    private const string StateExpression = """
        CASE
            WHEN u."Status" = 'InFlight' THEN 'Fetching'
            WHEN u."Status" = 'Idle' AND u."FailCount" > 0 THEN 'Retrying'
            WHEN u."Status" = 'Idle' AND u."LastCrawlAt" IS NULL THEN 'Queued'
            WHEN u."Status" = 'Disabled' AND u."FailReason" IN ('robots', 'blocked-address', 'redirect') THEN 'Skipped'
            WHEN u."Status" = 'Disabled' AND u."FailReason" IS NOT NULL THEN 'Failed'
            WHEN f."Id" IS NULL THEN 'Queued'
            WHEN f."StatusCode" NOT BETWEEN 200 AND 299 THEN 'Failed'
            WHEN f."ObjectName" IS NULL THEN 'Skipped'
            WHEN d."Id" IS NULL THEN 'Parsing'
            WHEN d."IndexedAt" IS NULL THEN 'Indexing'
            WHEN (d."Policy"::jsonb ->> 'index') = 'false'
              OR (d."Policy"::jsonb ->> 'indexUntil')::timestamptz <= @Now THEN 'NotIndexed'
            ELSE 'Indexed'
        END
        """;

    private const string Rows = $"""
        SELECT u."Id", u."CrawlJobId", u."Url", u."Host", u."Depth", u."FailReason", u."FailCount", u."LastCrawlAt", u."NextCrawlAt",
               f."StatusCode"::int AS "StatusCode", f."ContentType", f."ContentLength", d."Title", d."Policy",
               {StateExpression} AS "State"
        FROM "{TableNames.DiscoveredUrls}" u
        LEFT JOIN "{TableNames.UrlFetches}" f ON f."Id" = u."LastFetchId"
        LEFT JOIN "{TableNames.ParsedDocuments}" d ON d."UrlFetchId" = f."Id"
        """;

    private const string Filtered = $"""
        FROM ({Rows} WHERE u."CrawlJobId" = @JobId) l
        WHERE (@States::text[] IS NULL OR l."State" = ANY(@States))
          AND (@Pattern::text IS NULL OR l."Url" ILIKE @Pattern ESCAPE '\' OR l."Title" ILIKE @Pattern ESCAPE '\')
        """;

    private const string ListSql = $"""SELECT l.* {Filtered} ORDER BY l."Id" LIMIT @Limit OFFSET @Offset""";

    private const string CountSql = $"""SELECT count(*)::int {Filtered}""";

    private const string SummarySql = $"""
        SELECT l."CrawlJobId" AS "JobId", l."State",
               CASE WHEN l."State" IN ('Failed', 'Retrying') THEN l."FailReason" END AS "Reason", count(*)::int AS "Count"
        FROM ({Rows} WHERE u."CrawlJobId" = ANY(@JobIds)) l
        GROUP BY 1, 2, 3
        """;

    private static readonly Dictionary<JobLinkFilter, LinkState[]?> FilterStates = new()
    {
        [JobLinkFilter.All] = null,
        [JobLinkFilter.Problems] = [LinkState.Failed, LinkState.Retrying],
        [JobLinkFilter.NotIndexed] = [.. Enum.GetValues<LinkState>().Where(state => state != LinkState.Indexed)],
        [JobLinkFilter.Pending] = [LinkState.Queued, LinkState.Fetching, LinkState.Retrying, LinkState.Parsing, LinkState.Indexing],
        [JobLinkFilter.Indexed] = [LinkState.Indexed]
    };

    private sealed record SummaryRow(int JobId, LinkState State, string? Reason, int Count);

    static PostgresJobLinkReader() => DapperTypeHandlers.Register();

    public async Task<PagedResult<JobLinkFacts>> List(int crawlJobId, JobLinkQuery query, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new
        {
            JobId = crawlJobId,
            States = FilterStates[query.Filter]?.Select(state => state.ToString()).ToArray(),
            Pattern = LikePattern.Contains(query.Query),
            query.Limit,
            query.Offset,
            Now = timeProvider.GetUtcNow()
        };
        var rows = await connection.QueryAsync<JobLinkFacts>(new CommandDefinition(ListSql, parameters, cancellationToken: ct));
        var total = await connection.ExecuteScalarAsync<int>(new CommandDefinition(CountSql, parameters, cancellationToken: ct));
        return new PagedResult<JobLinkFacts>(total, rows.ToList());
    }

    public async Task<IReadOnlyDictionary<int, JobLinkSummary>> Summarize(IReadOnlyList<int> crawlJobIds, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { JobIds = crawlJobIds.ToArray(), Now = timeProvider.GetUtcNow() };
        var rows = await connection.QueryAsync<SummaryRow>(new CommandDefinition(SummarySql, parameters, cancellationToken: ct));
        return rows.GroupBy(row => row.JobId).ToDictionary(group => group.Key, group => ToSummary(group.ToList()));
    }

    private static JobLinkSummary ToSummary(IReadOnlyList<SummaryRow> rows)
    {
        var counts = rows.GroupBy(row => row.State).ToDictionary(group => group.Key, group => group.Sum(row => row.Count));
        var problems = rows
            .Where(row => row.State is LinkState.Failed or LinkState.Retrying)
            .GroupBy(row => row.Reason ?? string.Empty)
            .Select(group => new LinkProblem(group.Key, LinkReasons.Label(group.Key), group.Sum(row => row.Count)))
            .OrderByDescending(problem => problem.Count)
            .ToList();
        return new JobLinkSummary(counts, problems);
    }
}
