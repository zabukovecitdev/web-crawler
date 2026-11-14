namespace SamoBot.Core.Domain;

/// <summary>
/// One link found on a page, ready to store. ToUrl is absolute and normalized exactly like the frontier's NormalizedUrl,
/// ToHost is the URI authority of the target and IsInternal says that it equals the authority of the page.
/// </summary>
public sealed record PageLink(string ToUrl, string ToHost, string? AnchorText, bool IsInternal, bool NoFollow);

/// <summary>The earliest page that links to a page.</summary>
public sealed record FoundOn(int PageId, string Url);

public enum LinkDirection
{
    Out,
    In
}

public sealed record LinkQuery(LinkDirection Direction, int Limit, int Offset);

/// <summary>
/// A row of the link list of a page. Direction out: Url and Text are the target and the anchor, PageId and StatusCode
/// describe the target when it is a known page. Direction in: they describe the linking page and the anchor it used.
/// </summary>
public sealed record PageLinkItem(string Url, string? Text, bool Internal, bool NoFollow, int? PageId, int? StatusCode);
