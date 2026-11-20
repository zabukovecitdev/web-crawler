namespace SamoBot.Core.Domain;

public sealed record PageLink(string ToUrl, string ToHost, string? AnchorText, bool IsInternal, bool NoFollow);

public sealed record FoundOn(int PageId, string Url);

public enum LinkDirection
{
    Out,
    In
}

public sealed record LinkQuery(LinkDirection Direction, int Limit, int Offset);

public sealed record PageLinkItem(string Url, string? Text, bool Internal, bool NoFollow, int? PageId, int? StatusCode);
