using Dapper;
using Npgsql;
using SamoBot.Core.Domain;
using SamoBot.Core.Ports;

namespace SamoBot.Adapters.Postgres;

/// <summary>Read-only queries about sites. A site is a host.</summary>
public sealed class PostgresSiteReader(NpgsqlDataSource dataSource) : ISiteReader
{
    public async Task<SitesPage> ListSites(SiteQuery query, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var filter = new { Pattern = LikePattern.Contains(query.Query), query.Limit, query.Offset };
        var items = await connection.QueryAsync<SiteSummary>(new CommandDefinition(SiteSql.List(query.Sort), filter, cancellationToken: ct));
        var total = await connection.ExecuteScalarAsync<int>(new CommandDefinition(SiteSql.Count, filter, cancellationToken: ct));
        var overview = await connection.QuerySingleAsync<SitesOverview>(new CommandDefinition(SiteSql.Overview, cancellationToken: ct));
        return new SitesPage(total, items.ToList(), overview);
    }

    public async Task<SiteDetail?> FindSite(string host, CancellationToken ct)
    {
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        var parameters = new { Host = host };
        var summary = await connection.QuerySingleOrDefaultAsync<SiteSummary>(new CommandDefinition(SiteSql.Find, parameters, cancellationToken: ct));
        if (summary is null)
        {
            return null;
        }

        var firstCrawlAt = await connection.QuerySingleAsync<DateTimeOffset?>(new CommandDefinition(SiteSql.FirstCrawl, parameters, cancellationToken: ct));
        var codes = await connection.QueryAsync<StatusCount>(new CommandDefinition(SiteSql.StatusCodes, parameters, cancellationToken: ct));
        var types = await connection.QueryAsync<ContentTypeCount>(new CommandDefinition(SiteSql.ContentTypes, parameters, cancellationToken: ct));
        return WithBreakdown(summary, firstCrawlAt, codes.ToList(), types.ToList());
    }

    private static SiteDetail WithBreakdown(
        SiteSummary summary,
        DateTimeOffset? firstCrawlAt,
        IReadOnlyList<StatusCount> codes,
        IReadOnlyList<ContentTypeCount> types) => new()
    {
        Host = summary.Host,
        Status = summary.Status,
        Pages = summary.Pages,
        Failed = summary.Failed,
        AvgResponseMs = summary.AvgResponseMs,
        CrawlDelayMs = summary.CrawlDelayMs,
        LastCrawlAt = summary.LastCrawlAt,
        ActiveJobId = summary.ActiveJobId,
        FirstCrawlAt = firstCrawlAt,
        StatusCodes = codes,
        ContentTypes = types
    };
}
