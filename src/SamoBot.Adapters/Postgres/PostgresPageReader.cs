using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

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
        var counts = await connection.QuerySingleAsync<LinkCounts>(new CommandDefinition(PageSql.LinkCounts, parameters, cancellationToken: ct));
        var foundOn = await connection.QuerySingleOrDefaultAsync<FoundOn>(new CommandDefinition(PageSql.FoundOn, parameters, cancellationToken: ct));
        return row.ToDetail(fetches.ToList(), counts, foundOn);
    }

    public async Task<PagedResult<PageLinkItem>?> ListLinks(int id, LinkQuery query, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { Id = id, query.Limit, query.Offset };
        if (!await connection.ExecuteScalarAsync<bool>(new CommandDefinition(PageSql.PageExists, parameters, cancellationToken: ct)))
        {
            return null;
        }

        var outgoing = query.Direction == LinkDirection.Out;
        var rows = await connection.QueryAsync<PageLinkItem>(
            new CommandDefinition(outgoing ? PageSql.OutgoingLinks : PageSql.IncomingLinks, parameters, cancellationToken: ct));
        var total = await connection.ExecuteScalarAsync<int>(
            new CommandDefinition(outgoing ? PageSql.CountOutgoingLinks : PageSql.CountIncomingLinks, parameters, cancellationToken: ct));
        return new PagedResult<PageLinkItem>(total, rows.ToList());
    }

    public async Task<StoredCopy?> FindStoredCopy(int id, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var row = await connection.QuerySingleOrDefaultAsync<(string ObjectName, string Url, string? Policy, string? ContentType)?>(
            new CommandDefinition(PageSql.StoredCopy, new { Id = id }, cancellationToken: ct));
        return row is { } found
            ? new StoredCopy(found.ObjectName, new Uri(found.Url, UriKind.Absolute), PagePolicyJson.Read(found.Policy), found.ContentType)
            : null;
    }
}
