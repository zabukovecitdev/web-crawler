using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>Read-only queries about crawled pages.</summary>
public sealed class PostgresPageReader(NpgsqlDataSource dataSource) : IPageReader
{
    public async Task<PagedResult<PageSummary>> ListHostPages(string host, PageQuery query, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new
        {
            Host = host,
            Pattern = LikePattern.Contains(query.Query),
            State = query.State.ToString().ToLowerInvariant(),
            query.Limit,
            query.Offset
        };
        var rows = await connection.QueryAsync<PageRow>(new CommandDefinition(PageSql.ListHostPages, parameters, cancellationToken: ct));
        var total = await connection.ExecuteScalarAsync<int>(new CommandDefinition(PageSql.CountHostPages, parameters, cancellationToken: ct));
        return new PagedResult<PageSummary>(total, rows.Select(row => row.ToSummary()).ToList());
    }

    public async Task<PageDetail?> FindPage(int id, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { Id = id };
        var row = await connection.QuerySingleOrDefaultAsync<PageRow>(new CommandDefinition(PageSql.Find, parameters, cancellationToken: ct));
        if (row is null)
        {
            return null;
        }

        var fetches = await connection.QueryAsync<PageFetch>(new CommandDefinition(PageSql.Fetches, parameters, cancellationToken: ct));
        return row.ToDetail(fetches.ToList());
    }

    public async Task<string?> FindContentObjectName(int id, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        return await connection.ExecuteScalarAsync<string?>(new CommandDefinition(PageSql.ContentObjectName, new { Id = id }, cancellationToken: ct));
    }
}
