using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>Read-only crawl analytics over UrlFetches.</summary>
public sealed class PostgresAnalyticsReader(NpgsqlDataSource dataSource, TimeProvider timeProvider) : IAnalyticsReader
{
    public async Task<AnalyticsOverview> Overview(AnalyticsRange range, CancellationToken ct)
    {
        var window = Window(range);
        var row = await QuerySingle<AnalyticsOverview>(AnalyticsSql.Overview, Parameters(window, null), ct);
        return row with { From = window.From, To = window.To };
    }

    public async Task<IReadOnlyList<HostAnalytics>> Hosts(AnalyticsRange range, CancellationToken ct) =>
        await Query<HostAnalytics>(AnalyticsSql.Hosts, Parameters(Window(range), null), ct);

    public Task<StatusClassCounts> StatusCodes(AnalyticsRange range, string? host, CancellationToken ct) =>
        QuerySingle<StatusClassCounts>(AnalyticsSql.StatusClasses, Parameters(Window(range), host), ct);

    public async Task<IReadOnlyList<AnalyticsContentType>> ContentTypes(AnalyticsRange range, string? host, CancellationToken ct)
    {
        var rows = await Query<AnalyticsContentType>(AnalyticsSql.ContentTypes, Parameters(Window(range), host), ct);
        return rows.Select(r => r with { Stored = StoredMediaTypes.IsStored(r.ContentType) }).ToList();
    }

    public async Task<IReadOnlyList<TimeseriesPoint>> Timeseries(AnalyticsRange range, string? host, AnalyticsBucket bucket, CancellationToken ct)
    {
        var window = Window(range);
        var parameters = new
        {
            window.From,
            window.To,
            Host = Normalize(host),
            Unit = bucket == AnalyticsBucket.Hour ? "hour" : "day",
            Floor = AnalyticsNames.Floor(window.To, bucket)
        };
        return await Query<TimeseriesPoint>(AnalyticsSql.Timeseries, parameters, ct);
    }

    private AnalyticsWindow Window(AnalyticsRange range) => AnalyticsWindow.For(range, timeProvider.GetUtcNow());

    private static string? Normalize(string? host) => string.IsNullOrWhiteSpace(host) ? null : host.Trim().ToLowerInvariant();

    private static object Parameters(AnalyticsWindow window, string? host) => new { window.From, Host = Normalize(host) };

    private async Task<IReadOnlyList<T>> Query<T>(string sql, object parameters, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return (await connection.QueryAsync<T>(new CommandDefinition(sql, parameters, cancellationToken: ct))).ToList();
    }

    private async Task<T> QuerySingle<T>(string sql, object parameters, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await connection.QuerySingleAsync<T>(new CommandDefinition(sql, parameters, cancellationToken: ct));
    }
}
