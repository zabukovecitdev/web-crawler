using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Read model of crawled pages for the dashboard.</summary>
public interface IPageReader
{
    Task<PagedResult<PageSummary>> ListHostPages(string host, PageQuery query, CancellationToken ct);

    Task<PageDetail?> FindPage(int id, CancellationToken ct);

    /// <summary>The object name of the newest stored body of a page, or null when no body is stored.</summary>
    Task<string?> FindContentObjectName(int id, CancellationToken ct);
}
