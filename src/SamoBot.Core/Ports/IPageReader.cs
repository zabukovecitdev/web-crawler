using SamoBot.Core.Domain;

namespace SamoBot.Core.Ports;

public interface IPageReader
{
    Task<PagedResult<PageSummary>> ListHostPages(string host, PageQuery query, CancellationToken ct);

    Task<PageDetail?> FindPage(int id, CancellationToken ct);

    Task<PagedResult<PageLinkItem>?> ListLinks(int id, LinkQuery query, CancellationToken ct);

    Task<StoredCopy?> FindStoredCopy(int id, CancellationToken ct);
}
