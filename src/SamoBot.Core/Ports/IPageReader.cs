using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

/// <summary>Read model of crawled pages for the dashboard.</summary>
public interface IPageReader
{
    Task<PagedResult<PageSummary>> ListHostPages(string host, PageQuery query, CancellationToken ct);

    Task<PageDetail?> FindPage(int id, CancellationToken ct);

    /// <summary>The links of a page (direction out) or the pages that link to it (direction in). Null when the page is unknown.</summary>
    Task<PagedResult<PageLinkItem>?> ListLinks(int id, LinkQuery query, CancellationToken ct);

    /// <summary>The newest stored body of a page with the policy parsed from it, or null when no body is stored.</summary>
    Task<StoredCopy?> FindStoredCopy(int id, CancellationToken ct);
}
