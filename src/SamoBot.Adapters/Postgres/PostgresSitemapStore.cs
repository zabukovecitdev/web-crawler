using Dapper;
using Npgsql;
using NpgsqlTypes;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;
using SamoBot.Core.Sitemaps;

namespace SamoBot.Adapters.Postgres;

/// <summary>
/// The sitemap queue and storage. One row in SitemapRequests per host is the queue. A claim sets a lease with
/// FOR UPDATE SKIP LOCKED, like the other work queues. A refresh replaces the files of a host in one transaction.
/// </summary>
public sealed class PostgresSitemapStore(NpgsqlDataSource dataSource, TimeProvider timeProvider) : ISitemapStore
{
    private const string EnqueueFinishedSql = $"""
        INSERT INTO "{TableNames.SitemapRequests}" ("Host", "RequestedAt")
        SELECT h."Host", @Now
        FROM "{TableNames.Hosts}" h
        LEFT JOIN "{TableNames.SitemapRequests}" r ON r."Host" = h."Host"
        WHERE (r."Host" IS NULL OR r."CompletedAt" < @StaleBefore
               OR (r."CompletedAt" IS NULL AND r."Attempts" >= @MaxAttempts AND r."RequestedAt" < @StaleBefore))
          AND EXISTS (
              SELECT 1 FROM "{TableNames.DiscoveredUrls}" u
              JOIN "{TableNames.CrawlJobs}" j ON j."Id" = u."CrawlJobId"
              WHERE u."Host" = h."Host" AND j."Status" = 'Completed')
        ON CONFLICT ("Host") DO UPDATE SET "RequestedAt" = EXCLUDED."RequestedAt", "CompletedAt" = NULL, "LeaseUntil" = NULL, "Attempts" = 0
        """;

    private const string EnqueueSql = $"""
        INSERT INTO "{TableNames.SitemapRequests}" ("Host", "RequestedAt") VALUES (@Host, @Now)
        ON CONFLICT ("Host") DO UPDATE SET "RequestedAt" = EXCLUDED."RequestedAt", "CompletedAt" = NULL, "Attempts" = 0
        """;

    private const string ClaimSql = $"""
        UPDATE "{TableNames.SitemapRequests}" r
        SET "LeaseUntil" = @Now + make_interval(secs => @LeaseSeconds), "Attempts" = r."Attempts" + 1
        WHERE r."Host" = (
            SELECT q."Host" FROM "{TableNames.SitemapRequests}" q
            WHERE q."CompletedAt" IS NULL AND q."Attempts" < @MaxAttempts AND (q."LeaseUntil" IS NULL OR q."LeaseUntil" < @Now)
            ORDER BY q."RequestedAt"
            LIMIT 1
            FOR UPDATE SKIP LOCKED)
        RETURNING r."Host", r."LeaseUntil"
        """;

    // One statement. The row lock of the UPDATE serializes callers, and RETURNING sees the new value, so the booked slot is the new
    // NextAllowedAt minus the delay. A missing host row returns no row, which means no wait.
    private const string ReserveSql = $"""
        UPDATE "{TableNames.Hosts}"
        SET "NextAllowedAt" = GREATEST("NextAllowedAt", @Now) + make_interval(secs => "CrawlDelayMs" / 1000.0)
        WHERE "Host" = @Host
        RETURNING extract(epoch FROM (("NextAllowedAt" - make_interval(secs => "CrawlDelayMs" / 1000.0)) - @Now))::float8
        """;

    private const string LockRequestSql = $"""SELECT "LeaseUntil" FROM "{TableNames.SitemapRequests}" WHERE "Host" = @Host FOR UPDATE""";

    private const string DeleteUrlsSql = $"""DELETE FROM "{TableNames.SitemapUrls}" WHERE "Host" = @Host""";

    private const string DeleteFilesSql = $"""DELETE FROM "{TableNames.SitemapFiles}" WHERE "Host" = @Host""";

    private const string InsertFileSql = $"""
        INSERT INTO "{TableNames.SitemapFiles}" ("Host", "Url", "ParentUrl", "Kind", "UrlCount", "FetchedAt")
        VALUES (@Host, @Url, @ParentUrl, @Kind, @UrlCount, @FetchedAt) RETURNING "Id"
        """;

    private const string CopyUrlsSql =
        $"""COPY "{TableNames.SitemapUrls}" ("Host", "Loc", "NormalizedLoc", "LastModified", "SitemapFileId") FROM STDIN (FORMAT BINARY)""";

    // A request made after the claim stays queued (CompletedAt NULL), so the refresh it asked for is not lost.
    private const string CompleteSql = $"""
        UPDATE "{TableNames.SitemapRequests}"
        SET "CompletedAt" = CASE WHEN "RequestedAt" <= @ClaimedAt THEN @Now ELSE NULL END, "LeaseUntil" = NULL, "Attempts" = 0
        WHERE "Host" = @Host
        """;

    static PostgresSitemapStore() => DapperTypeHandlers.Register();

    public async Task EnqueueFinishedHosts(TimeSpan refreshAfter, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var now = timeProvider.GetUtcNow();
        await connection.ExecuteAsync(new CommandDefinition(EnqueueFinishedSql, new { Now = now, StaleBefore = now - refreshAfter, MaxAttempts = SitemapBatchHandler.MaxAttempts }, cancellationToken: ct));
    }

    public async Task Enqueue(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(EnqueueSql, new { Host = host, Now = timeProvider.GetUtcNow() }, cancellationToken: ct));
    }

    public async Task<SitemapClaim?> ClaimNext(TimeSpan lease, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var now = timeProvider.GetUtcNow();
        var parameters = new { Now = now, LeaseSeconds = lease.TotalSeconds, MaxAttempts = SitemapBatchHandler.MaxAttempts };
        var claimed = await connection.QuerySingleOrDefaultAsync<(string? Host, DateTimeOffset LeaseUntil)>(
            new CommandDefinition(ClaimSql, parameters, cancellationToken: ct));
        return claimed.Host is { } host ? new SitemapClaim(host, now, claimed.LeaseUntil) : null;
    }

    public async Task<TimeSpan> ReserveFetchSlot(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var seconds = await connection.QuerySingleOrDefaultAsync<double?>(
            new CommandDefinition(ReserveSql, new { Host = host, Now = timeProvider.GetUtcNow() }, cancellationToken: ct));
        return seconds is > 0 ? TimeSpan.FromSeconds(seconds.Value) : TimeSpan.Zero;
    }

    public async Task<bool> Complete(SitemapClaim claim, SitemapWalkResult result, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        var leaseUntil = await connection.QuerySingleOrDefaultAsync<DateTime?>(
            new CommandDefinition(LockRequestSql, new { claim.Host }, transaction, cancellationToken: ct));
        if (AsUtcOffset(leaseUntil) != claim.LeaseUntil)
        {
            await transaction.RollbackAsync(ct);
            return false;
        }

        if (result.Files.Count > 0 || result.Gone)
        {
            await Replace(connection, transaction, claim.Host, result.Files, ct);
        }

        var parameters = new { claim.Host, claim.ClaimedAt, Now = timeProvider.GetUtcNow() };
        await connection.ExecuteAsync(new CommandDefinition(CompleteSql, parameters, transaction, cancellationToken: ct));
        await transaction.CommitAsync(ct);
        return true;
    }

    private static DateTimeOffset? AsUtcOffset(DateTime? value) =>
        value is null ? null : new DateTimeOffset(DateTime.SpecifyKind(value.Value, DateTimeKind.Utc));

    private static async Task Replace(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string host,
        IReadOnlyList<SitemapFileContent> files,
        CancellationToken ct)
    {
        var parameters = new { Host = host };
        await connection.ExecuteAsync(new CommandDefinition(DeleteUrlsSql, parameters, transaction, cancellationToken: ct));
        await connection.ExecuteAsync(new CommandDefinition(DeleteFilesSql, parameters, transaction, cancellationToken: ct));
        foreach (var content in files)
        {
            var fileId = await InsertFile(connection, transaction, content.File, ct);
            await CopyUrls(connection, host, fileId, content.Urls, ct);
        }
    }

    private static Task<int> InsertFile(NpgsqlConnection connection, NpgsqlTransaction transaction, SitemapFile file, CancellationToken ct)
    {
        var parameters = new
        {
            file.Host,
            file.Url,
            file.ParentUrl,
            Kind = file.Kind.ToString(),
            file.UrlCount,
            file.FetchedAt
        };

        return connection.ExecuteScalarAsync<int>(new CommandDefinition(InsertFileSql, parameters, transaction, cancellationToken: ct));
    }

    private static async Task CopyUrls(NpgsqlConnection connection, string host, int fileId, IReadOnlyList<SitemapEntry> urls, CancellationToken ct)
    {
        if (urls.Count == 0)
        {
            return;
        }

        await using var writer = await connection.BeginBinaryImportAsync(CopyUrlsSql, ct);
        foreach (var url in urls)
        {
            await writer.StartRowAsync(ct);
            await writer.WriteAsync(host, NpgsqlDbType.Varchar, ct);
            await writer.WriteAsync(url.Loc, NpgsqlDbType.Varchar, ct);
            await writer.WriteAsync(url.NormalizedLoc, NpgsqlDbType.Varchar, ct);
            await WriteModified(writer, url.LastModified, ct);
            await writer.WriteAsync(fileId, NpgsqlDbType.Integer, ct);
        }

        await writer.CompleteAsync(ct);
    }

    private static Task WriteModified(NpgsqlBinaryImporter writer, DateTimeOffset? value, CancellationToken ct) =>
        value is { } date ? writer.WriteAsync(date.UtcDateTime, NpgsqlDbType.TimestampTz, ct) : writer.WriteNullAsync(ct);
}
