using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// The URL frontier in PostgreSQL. A claim takes at most one due URL for each host and locks the host row,
/// so that two workers never claim the same host together. All times come from <see cref="TimeProvider"/>.
/// </summary>
public sealed class PostgresUrlFrontier(NpgsqlDataSource dataSource, TimeProvider timeProvider) : IUrlFrontier
{
    private const string ClaimSql = $"""
        WITH due AS (
            SELECT DISTINCT ON (u."Host") u."Id", u."Host", h."CrawlDelayMs", h."AvgResponseMs", h."ConsecutiveFailures"
            FROM "{TableNames.DiscoveredUrls}" u
            JOIN "{TableNames.Hosts}" h ON h."Host" = u."Host"
            JOIN "{TableNames.CrawlJobs}" j ON j."Id" = u."CrawlJobId" AND j."Status" = 'Running'
            WHERE h."NextAllowedAt" <= @Now
              AND ((u."Status" = 'Idle' AND COALESCE(u."NextCrawlAt", @Now) <= @Now)
                OR (u."Status" = 'InFlight' AND u."LeaseUntil" < @Now))
            ORDER BY u."Host", u."Depth", u."Id"
        ),
        locked AS (
            SELECT h."Host" FROM "{TableNames.Hosts}" h
            WHERE h."Host" IN (SELECT "Host" FROM due)
            ORDER BY h."NextAllowedAt"
            LIMIT @BatchSize
            FOR UPDATE OF h SKIP LOCKED
        ),
        picked AS (
            SELECT d."Id", d."Host", d."CrawlDelayMs", d."AvgResponseMs", d."ConsecutiveFailures" FROM due d JOIN locked l ON l."Host" = d."Host"
        ),
        bump AS (
            UPDATE "{TableNames.Hosts}" h
            SET "NextAllowedAt" = @Now + make_interval(secs => @HoldSeconds)
            FROM picked p WHERE p."Host" = h."Host"
        )
        UPDATE "{TableNames.DiscoveredUrls}" u
        SET "Status" = 'InFlight', "LeaseUntil" = @Now + make_interval(secs => @LeaseSeconds), "StatusUpdatedAt" = @Now
        FROM picked p
        WHERE p."Id" = u."Id"
          AND (u."Status" = 'Idle' OR (u."Status" = 'InFlight' AND u."LeaseUntil" < @Now))
        RETURNING u."Id", u."CrawlJobId", u."Host", u."Url", u."NormalizedUrl", u."Depth", u."FailCount",
                  u."UseJsRendering", p."CrawlDelayMs", p."AvgResponseMs", p."ConsecutiveFailures"
        """;

    private const string InsertFetchSql = $"""
        INSERT INTO "{TableNames.UrlFetches}"
            ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ContentLength", "ResponseTimeMs", "ObjectName", "FinalUrl", "ResponseHeaders")
        VALUES (@UrlId, @FetchedAt, @StatusCode, @ContentType, @ContentLength, @ResponseTimeMs, @ObjectName, @FinalUrl, @ResponseHeaders::text)
        RETURNING "Id"
        """;

    // Releasing the in-flight hold: the wait after a request is booked from the time of the completion, and it REPLACES the
    // hold that the claim booked (GREATEST with the hold would keep the host closed for the whole hold).
    // A value that is still within the hold (at most @HeldUntil, plus 1 ms for rounding) is the claim's own hold, or an older
    // wait: it is replaced. A later value is a wait that someone booked on purpose while the request ran (a sitemap fetch that
    // queued behind it, or the hold of a second claim after this one expired): it stays, so the release never moves a host
    // backwards past a wait it must keep. The new wait already holds a Retry-After and the adaptive delay (computed by the caller).
    // Without a known hold (@HeldUntil null) the later value always stays.
    private const string ReleasedWait = """
        CASE WHEN "NextAllowedAt" <= @HeldUntil::timestamptz + make_interval(secs => 0.001)
             THEN @Now + make_interval(secs => @DelaySeconds)
             ELSE GREATEST("NextAllowedAt", @Now + make_interval(secs => @DelaySeconds)) END
        """;

    // The host row is updated first, like the claim locks the host before its URLs, so that the two never wait for each other.
    private const string UpdateHostSql = $"""
        UPDATE "{TableNames.Hosts}"
        SET "NextAllowedAt" = {ReleasedWait},
            "AvgResponseMs" = @AvgResponseMs, "ConsecutiveFailures" = @ConsecutiveFailures, "LastDelayMs" = @DelayMs
        WHERE "Host" = @Host
        """;

    // An outcome with no pacing change (blocked address, robots disallow) still releases the hold, with the base delay.
    private const string ReleaseHostSql = $"""
        UPDATE "{TableNames.Hosts}" SET "NextAllowedAt" = {ReleasedWait} WHERE "Host" = @Host
        """;

    private const string UpdateUrlSql = $"""
        UPDATE "{TableNames.DiscoveredUrls}"
        SET "Status" = @Status, "NextCrawlAt" = @NextCrawlAt, "FailCount" = @FailCount, "FailReason" = @FailReason,
            "LeaseUntil" = NULL, "StatusUpdatedAt" = @Now,
            "LastCrawlAt" = CASE WHEN @FetchId IS NULL THEN "LastCrawlAt" ELSE @Now END,
            "LastFetchId" = COALESCE(@FetchId, "LastFetchId")
        WHERE "Id" = @UrlId
        """;

    // A requeued link looks like a link that was never fetched (no last fetch), so the queue counters, the completion
    // check and the dashboard all see it as waiting. Its fetch history stays in UrlFetches.
    private const string RequeueSql = $"""
        UPDATE "{TableNames.DiscoveredUrls}"
        SET "Status" = 'Idle', "NextCrawlAt" = @Now, "FailCount" = 0, "FailReason" = NULL, "LeaseUntil" = NULL,
            "LastCrawlAt" = NULL, "LastFetchId" = NULL, "StatusUpdatedAt" = @Now
        WHERE "CrawlJobId" = @JobId
          AND "Status" <> 'InFlight'
          AND (@UrlId::int IS NULL OR "Id" = @UrlId)
          AND (NOT @FailedOnly
               OR ("Status" = 'Idle' AND "FailCount" > 0)
               OR ("Status" = 'Disabled' AND "FailReason" NOT IN ('redirect', 'robots', 'blocked-address')))
        """;

    public async Task<int> Requeue(int crawlJobId, RequeueScope scope, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { JobId = crawlJobId, scope.UrlId, scope.FailedOnly, Now = timeProvider.GetUtcNow() };
        return await connection.ExecuteAsync(new CommandDefinition(RequeueSql, parameters, cancellationToken: ct));
    }

    public async Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, TimeSpan hold, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var now = timeProvider.GetUtcNow();
        var parameters = new { Now = now, BatchSize = batchSize, LeaseSeconds = lease.TotalSeconds, HoldSeconds = hold.TotalSeconds };
        var urls = await connection.QueryAsync<ClaimedUrl>(new CommandDefinition(ClaimSql, parameters, cancellationToken: ct));
        return urls.Select(url => url with { HeldUntil = now + hold }).ToList();
    }

    public async Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var now = timeProvider.GetUtcNow();

        await ReleaseHost(connection, transaction, url, update.Pacing, now, ct);
        var fetchId = fetch is null ? (int?)null : await InsertFetch(connection, transaction, url, fetch, ct);
        await UpdateUrl(connection, transaction, UpdateParameters(url, update, fetchId, now), ct);
        if (update.NewUrl is not null)
        {
            await NewUrlInserter.Insert(connection, transaction, url.CrawlJobId, [update.NewUrl], now, ct);
        }

        await transaction.CommitAsync(ct);
    }

    // LastCrawlAt goes back to null: the URL waits for its first fetch again, so the job does not complete before the fetch.
    private const string RefetchSkippedSql = $"""
        UPDATE "{TableNames.DiscoveredUrls}" u
        SET "NextCrawlAt" = @Now, "LastCrawlAt" = NULL, "StatusUpdatedAt" = @Now
        FROM "{TableNames.UrlFetches}" f
        WHERE u."CrawlJobId" = @CrawlJobId AND u."Status" = 'Idle' AND f."Id" = u."LastFetchId"
          AND f."StatusCode" BETWEEN 200 AND 299 AND f."ObjectName" IS NULL AND f."ContentLength" > 0
          AND lower(btrim(split_part(f."ContentType", ';', 1))) = ANY(@MediaTypes)
        """;

    private const string ReparseStoredSql = $"""
        UPDATE "{TableNames.UrlFetches}" f
        SET "ParsedAt" = NULL, "LeaseUntil" = NULL
        FROM "{TableNames.DiscoveredUrls}" u
        WHERE u."CrawlJobId" = @CrawlJobId AND f."Id" = u."LastFetchId"
          AND f."ObjectName" IS NOT NULL AND f."ParsedAt" IS NOT NULL
        """;

    public async Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await NewUrlInserter.Insert(connection, transaction, crawlJobId, seeds, timeProvider.GetUtcNow(), ct);
        await transaction.CommitAsync(ct);
    }

    public async Task<RequeuedWork> RequeueForReprocess(int crawlJobId, IReadOnlyList<string> storedMediaTypes, CancellationToken ct)
    {
        var parameters = new { CrawlJobId = crawlJobId, MediaTypes = storedMediaTypes.ToArray(), Now = timeProvider.GetUtcNow() };
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var refetch = await connection.ExecuteAsync(new CommandDefinition(RefetchSkippedSql, parameters, transaction, cancellationToken: ct));
        var reparse = await connection.ExecuteAsync(new CommandDefinition(ReparseStoredSql, parameters, transaction, cancellationToken: ct));
        await transaction.CommitAsync(ct);
        return new RequeuedWork(refetch, reparse);
    }

    /// <summary>
    /// Every outcome releases the in-flight hold of the host, in the same transaction as the result. The new wait is
    /// completion time + the delay of the pacing (base delay when the pacing is unchanged). It replaces the hold, and it
    /// never lowers a later wait that was booked on purpose (see <see cref="ReleasedWait"/>).
    /// </summary>
    private static Task<int> ReleaseHost(
        NpgsqlConnection connection, NpgsqlTransaction transaction, ClaimedUrl url, HostPacingUpdate? pacing, DateTimeOffset now, CancellationToken ct)
    {
        var delay = pacing?.Delay ?? TimeSpan.FromMilliseconds(url.CrawlDelayMs);
        var parameters = new
        {
            url.Host,
            Now = now,
            url.HeldUntil,
            DelaySeconds = delay.TotalSeconds,
            DelayMs = (int)delay.TotalMilliseconds,
            pacing?.AvgResponseMs,
            ConsecutiveFailures = pacing?.ConsecutiveFailures ?? 0
        };
        var sql = pacing is null ? ReleaseHostSql : UpdateHostSql;
        return connection.ExecuteAsync(new CommandDefinition(sql, parameters, transaction, cancellationToken: ct));
    }

    private static Task<int> InsertFetch(
        NpgsqlConnection connection, NpgsqlTransaction transaction, ClaimedUrl url, FetchRecord fetch, CancellationToken ct)
    {
        var parameters = new
        {
            UrlId = url.Id,
            fetch.FetchedAt,
            fetch.StatusCode,
            fetch.ContentType,
            fetch.ContentLength,
            fetch.ResponseTimeMs,
            fetch.ObjectName,
            fetch.FinalUrl,
            ResponseHeaders = ResponseHeadersJson.Write(fetch.ResponseHeaders)
        };
        return connection.ExecuteScalarAsync<int>(new CommandDefinition(InsertFetchSql, parameters, transaction, cancellationToken: ct));
    }

    private static Task<int> UpdateUrl(
        NpgsqlConnection connection, NpgsqlTransaction transaction, object parameters, CancellationToken ct) =>
        connection.ExecuteAsync(new CommandDefinition(UpdateUrlSql, parameters, transaction, cancellationToken: ct));

    private static object UpdateParameters(ClaimedUrl url, UrlUpdate update, int? fetchId, DateTimeOffset now) => new
    {
        UrlId = url.Id,
        Status = update.Status.ToString(),
        update.NextCrawlAt,
        update.FailCount,
        update.FailReason,
        FetchId = fetchId,
        Now = now
    };
}
