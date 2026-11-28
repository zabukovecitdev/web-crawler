using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

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
            ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ContentLength", "BodyTruncated", "ResponseTimeMs", "ObjectName",
             "FinalUrl", "ResponseHeaders")
        VALUES (@UrlId, @FetchedAt, @StatusCode, @ContentType, @ContentLength, @BodyTruncated, @ResponseTimeMs, @ObjectName,
                @FinalUrl, @ResponseHeaders::text)
        RETURNING "Id"
        """;

    private const string ReleasedWait = """
        CASE WHEN "NextAllowedAt" <= @HeldUntil::timestamptz + make_interval(secs => 0.001)
             THEN @Now + make_interval(secs => @DelaySeconds)
             ELSE GREATEST("NextAllowedAt", @Now + make_interval(secs => @DelaySeconds)) END
        """;

    private const string UpdateHostSql = $"""
        UPDATE "{TableNames.Hosts}"
        SET "NextAllowedAt" = {ReleasedWait},
            "AvgResponseMs" = @AvgResponseMs, "ConsecutiveFailures" = @ConsecutiveFailures, "LastDelayMs" = @DelayMs
        WHERE "Host" = @Host
        """;

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

    private const string RequeueSql = $"""
        UPDATE "{TableNames.DiscoveredUrls}"
        SET "Status" = 'Idle', "NextCrawlAt" = @Now, "FailCount" = 0, "FailReason" = NULL, "LeaseUntil" = NULL,
            "LastCrawlAt" = NULL, "StatusUpdatedAt" = @Now
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
            fetch.BodyTruncated,
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
