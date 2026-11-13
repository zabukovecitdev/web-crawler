using Dapper;
using Npgsql;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Postgres;

/// <summary>Direct SQL inserts for the integration tests. The tests do not use the adapters to prepare data.</summary>
public static class PostgresTestData
{
    public static Task<int> InsertJob(
        NpgsqlDataSource dataSource,
        string status = "Running",
        string allowedHosts = """["a.example"]""",
        int? maxUrls = null) =>
        Scalar<int>(
            dataSource,
            """
            INSERT INTO "CrawlJobs" ("Status", "AllowedHosts", "MaxUrls", "SeedUrls")
            VALUES (@status, @allowedHosts, @maxUrls, '[]') RETURNING "Id"
            """,
            new { status, allowedHosts, maxUrls });

    /// <summary>Inserts a URL and its host row. The host is due at the fake start time.</summary>
    public static async Task<int> InsertUrl(
        NpgsqlDataSource dataSource,
        int jobId,
        string url,
        int depth = 0,
        string status = "Idle",
        DateTimeOffset? nextCrawlAt = null)
    {
        var host = new Uri(url).Host;
        await using var connection = await dataSource.OpenConnectionAsync();
        await connection.ExecuteAsync(
            """INSERT INTO "Hosts" ("Host", "NextAllowedAt") VALUES (@host, @due) ON CONFLICT DO NOTHING""",
            new { host, due = TestData.Now });
        return await connection.ExecuteScalarAsync<int>(
            """
            INSERT INTO "DiscoveredUrls" ("Host", "Url", "NormalizedUrl", "Status", "NextCrawlAt", "CrawlJobId", "Depth")
            VALUES (@host, @url, @url, @status, @next, @jobId, @depth) RETURNING "Id"
            """,
            new { host, url, status, next = nextCrawlAt ?? TestData.Now, jobId, depth });
    }

    /// <summary>Inserts a fetch with stored HTML for a URL. The parser can claim it.</summary>
    public static Task<int> InsertFetch(NpgsqlDataSource dataSource, int urlId, string objectName = "obj", bool parsed = false) =>
        Scalar<int>(
            dataSource,
            """
            INSERT INTO "UrlFetches" ("DiscoveredUrlId", "FetchedAt", "StatusCode", "ContentType", "ObjectName", "ParsedAt")
            VALUES (@urlId, @fetchedAt, 200, 'text/html', @objectName, @parsedAt) RETURNING "Id"
            """,
            new { urlId, fetchedAt = TestData.Now, objectName, parsedAt = parsed ? TestData.Now : (DateTimeOffset?)null });

    public static Task<T> Scalar<T>(NpgsqlDataSource dataSource, string sql, object? parameters = null) =>
        Run(dataSource, async connection => (await connection.ExecuteScalarAsync<T>(sql, parameters))!);

    public static Task<dynamic> Row(NpgsqlDataSource dataSource, string sql, object? parameters = null) =>
        Run(dataSource, connection => connection.QuerySingleAsync(sql, parameters));

    public static Task<IReadOnlyList<T>> Rows<T>(NpgsqlDataSource dataSource, string sql, object? parameters = null) =>
        Run(dataSource, async connection => (IReadOnlyList<T>)(await connection.QueryAsync<T>(sql, parameters)).ToList());

    private static async Task<T> Run<T>(NpgsqlDataSource dataSource, Func<NpgsqlConnection, Task<T>> action)
    {
        await using var connection = await dataSource.OpenConnectionAsync();
        return await action(connection);
    }
}
