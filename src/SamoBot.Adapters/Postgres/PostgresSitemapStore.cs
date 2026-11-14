using Dapper;
using Npgsql;
using NpgsqlTypes;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

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
        WHERE (r."Host" IS NULL OR r."CompletedAt" < @StaleBefore)
          AND EXISTS (
              SELECT 1 FROM "{TableNames.DiscoveredUrls}" u
              JOIN "{TableNames.CrawlJobs}" j ON j."Id" = u."CrawlJobId"
              WHERE u."Host" = h."Host" AND j."Status" = 'Completed')
        ON CONFLICT ("Host") DO UPDATE SET "RequestedAt" = EXCLUDED."RequestedAt", "CompletedAt" = NULL, "LeaseUntil" = NULL
        """;

    private const string EnqueueSql = $"""
        INSERT INTO "{TableNames.SitemapRequests}" ("Host", "RequestedAt") VALUES (@Host, @Now)
        ON CONFLICT ("Host") DO UPDATE SET "RequestedAt" = EXCLUDED."RequestedAt", "CompletedAt" = NULL
        """;

    private const string ClaimSql = $"""
        UPDATE "{TableNames.SitemapRequests}" r
        SET "LeaseUntil" = @Now + make_interval(secs => @LeaseSeconds)
        WHERE r."Host" = (
            SELECT q."Host" FROM "{TableNames.SitemapRequests}" q
            WHERE q."CompletedAt" IS NULL AND (q."LeaseUntil" IS NULL OR q."LeaseUntil" < @Now)
            ORDER BY q."RequestedAt"
            LIMIT 1
            FOR UPDATE SKIP LOCKED)
        RETURNING r."Host"
        """;

    // Locks the host row, so that two workers never book the same slot. A missing host row means no wait.
    private const string ReserveSql = $"""
        WITH cur AS (
            SELECT "Host", "NextAllowedAt", "CrawlDelayMs" FROM "{TableNames.Hosts}" WHERE "Host" = @Host FOR UPDATE
        ),
        bump AS (
            UPDATE "{TableNames.Hosts}" h
            SET "NextAllowedAt" = GREATEST(c."NextAllowedAt", @Now) + make_interval(secs => c."CrawlDelayMs" / 1000.0)
            FROM cur c WHERE h."Host" = c."Host"
            RETURNING GREATEST(c."NextAllowedAt", @Now) AS "Slot"
        )
        SELECT extract(epoch FROM ("Slot" - @Now))::float8 FROM bump
        """;

    private const string DeleteUrlsSql = $"""DELETE FROM "{TableNames.SitemapUrls}" WHERE "Host" = @Host""";

    private const string DeleteFilesSql = $"""DELETE FROM "{TableNames.SitemapFiles}" WHERE "Host" = @Host""";

    private const string InsertFileSql = $"""
        INSERT INTO "{TableNames.SitemapFiles}" ("Host", "Url", "ParentUrl", "Kind", "UrlCount", "FetchedAt")
        VALUES (@Host, @Url, @ParentUrl, @Kind, @UrlCount, @FetchedAt) RETURNING "Id"
        """;

    private const string CopyUrlsSql =
        $"""COPY "{TableNames.SitemapUrls}" ("Host", "Loc", "NormalizedLoc", "LastModified", "SitemapFileId") FROM STDIN (FORMAT BINARY)""";

    private const string CompleteSql = $"""
        UPDATE "{TableNames.SitemapRequests}" SET "CompletedAt" = @Now, "LeaseUntil" = NULL WHERE "Host" = @Host
        """;

    static PostgresSitemapStore() => DapperTypeHandlers.Register();

    public async Task EnqueueFinishedHosts(TimeSpan refreshAfter, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var now = timeProvider.GetUtcNow();
        await connection.ExecuteAsync(new CommandDefinition(EnqueueFinishedSql, new { Now = now, StaleBefore = now - refreshAfter }, cancellationToken: ct));
    }

    public async Task Enqueue(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await connection.ExecuteAsync(new CommandDefinition(EnqueueSql, new { Host = host, Now = timeProvider.GetUtcNow() }, cancellationToken: ct));
    }

    public async Task<string?> ClaimNext(TimeSpan lease, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { Now = timeProvider.GetUtcNow(), LeaseSeconds = lease.TotalSeconds };
        return await connection.QuerySingleOrDefaultAsync<string>(new CommandDefinition(ClaimSql, parameters, cancellationToken: ct));
    }

    public async Task<TimeSpan> ReserveFetchSlot(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var seconds = await connection.QuerySingleOrDefaultAsync<double?>(
            new CommandDefinition(ReserveSql, new { Host = host, Now = timeProvider.GetUtcNow() }, cancellationToken: ct));
        return seconds is > 0 ? TimeSpan.FromSeconds(seconds.Value) : TimeSpan.Zero;
    }

    public async Task Complete(string host, IReadOnlyList<SitemapFileContent> files, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        if (files.Count > 0)
        {
            await Replace(connection, transaction, host, files, ct);
        }

        var parameters = new { Host = host, Now = timeProvider.GetUtcNow() };
        await connection.ExecuteAsync(new CommandDefinition(CompleteSql, parameters, transaction, cancellationToken: ct));
        await transaction.CommitAsync(ct);
    }

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
