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
            SELECT DISTINCT ON (u."Host") u."Id", u."Host", h."CrawlDelayMs"
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
            SELECT d."Id", d."Host", d."CrawlDelayMs" FROM due d JOIN locked l ON l."Host" = d."Host"
        ),
        bump AS (
            UPDATE "{TableNames.Hosts}" h
            SET "NextAllowedAt" = @Now + make_interval(secs => h."CrawlDelayMs" / 1000.0)
            FROM picked p WHERE p."Host" = h."Host"
        )
        UPDATE "{TableNames.DiscoveredUrls}" u
        SET "Status" = 'InFlight', "LeaseUntil" = @Now + make_interval(secs => @LeaseSeconds), "StatusUpdatedAt" = @Now
        FROM picked p
        WHERE p."Id" = u."Id"
          AND (u."Status" = 'Idle' OR (u."Status" = 'InFlight' AND u."LeaseUntil" < @Now))
        RETURNING u."Id", u."CrawlJobId", u."Host", u."Url", u."NormalizedUrl", u."Depth", u."FailCount",
                  u."UseJsRendering", u."RespectRobots", p."CrawlDelayMs"
        """;

    private const string InsertFetchSql = $"""
        INSERT INTO "{TableNames.UrlFetches}"
            ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ContentLength", "ResponseTimeMs", "ObjectName", "FinalUrl")
        VALUES (@UrlId, @FetchedAt, @StatusCode, @ContentType, @ContentLength, @ResponseTimeMs, @ObjectName, @FinalUrl)
        RETURNING "Id"
        """;

    private const string UpdateUrlSql = $"""
        UPDATE "{TableNames.DiscoveredUrls}"
        SET "Status" = @Status, "NextCrawlAt" = @NextCrawlAt, "FailCount" = @FailCount, "FailReason" = @FailReason,
            "LeaseUntil" = NULL, "StatusUpdatedAt" = @Now,
            "LastCrawlAt" = CASE WHEN @FetchId IS NULL THEN "LastCrawlAt" ELSE @Now END,
            "LastFetchId" = COALESCE(@FetchId, "LastFetchId")
        WHERE "Id" = @UrlId
        """;

    public async Task<IReadOnlyList<ClaimedUrl>> Claim(int batchSize, TimeSpan lease, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { Now = timeProvider.GetUtcNow(), BatchSize = batchSize, LeaseSeconds = lease.TotalSeconds };
        var urls = await connection.QueryAsync<ClaimedUrl>(new CommandDefinition(ClaimSql, parameters, cancellationToken: ct));
        return urls.ToList();
    }

    public async Task Complete(ClaimedUrl url, UrlUpdate update, FetchRecord? fetch, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var now = timeProvider.GetUtcNow();

        var fetchId = fetch is null ? (int?)null : await InsertFetch(connection, transaction, url, fetch, ct);
        await UpdateUrl(connection, transaction, UpdateParameters(url, update, fetchId, now), ct);
        if (update.NewUrl is not null)
        {
            await NewUrlInserter.Insert(connection, transaction, url.CrawlJobId, [update.NewUrl], now, ct);
        }

        await transaction.CommitAsync(ct);
    }

    public async Task AddSeeds(int crawlJobId, IReadOnlyList<NewUrl> seeds, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await NewUrlInserter.Insert(connection, transaction, crawlJobId, seeds, timeProvider.GetUtcNow(), ct);
        await transaction.CommitAsync(ct);
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
            fetch.FinalUrl
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
