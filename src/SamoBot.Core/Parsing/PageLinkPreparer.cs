using System.Text.RegularExpressions;
using SamoBot.Core.Directives;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Parsing;

/// <summary>
/// Turns the links of a page into the rows of the link table: every http and https link, internal or external,
/// whether the job follows it or not. Targets use the same normalization as the frontier (<see cref="LinkResolver"/>).
/// </summary>
public static partial class PageLinkPreparer
{
    /// <summary>At most this many distinct targets are stored for one page; the rest are dropped.</summary>
    public const int MaxLinksPerPage = 2000;

    /// <summary>A target longer than this is skipped (the same limit as the URL columns).</summary>
    public const int MaxUrlLength = 2048;

    public const int MaxAnchorLength = 200;

    /// <summary>The links of the page, resolved against its base URL. <paramref name="isNoFollow"/> marks the links the crawler does not follow.</summary>
    public static IReadOnlyList<PageLink> Prepare(PageContext page, Func<ParsedLink, bool> isNoFollow) =>
        Prepare(page.BaseUrl, page.PageUrl, page.Page.Links, isNoFollow);

    public static IReadOnlyList<PageLink> Prepare(Uri baseUrl, Uri pageUrl, IEnumerable<ParsedLink> links, Func<ParsedLink, bool> isNoFollow)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<PageLink>();
        foreach (var link in links)
        {
            if (result.Count >= MaxLinksPerPage)
            {
                break;
            }

            var prepared = Build(baseUrl, pageUrl, link, isNoFollow);
            if (prepared is not null && seen.Add(prepared.ToUrl))
            {
                result.Add(prepared);
            }
        }

        return result;
    }

    private static PageLink? Build(Uri baseUrl, Uri pageUrl, ParsedLink link, Func<ParsedLink, bool> isNoFollow)
    {
        if (link.Url.AsSpan().TrimStart().StartsWith("#"))
        {
            return null;
        }

        var target = LinkResolver.ResolveOne(baseUrl, link.Url);
        if (target is null || target.Length > MaxUrlLength)
        {
            return null;
        }

        var host = new Uri(target).Authority;
        return new PageLink(target, host, Anchor(link.LinkText), string.Equals(host, pageUrl.Authority, StringComparison.OrdinalIgnoreCase), isNoFollow(link));
    }

    private static string? Anchor(string text)
    {
        var collapsed = WhiteSpace().Replace(text ?? string.Empty, " ").Trim();
        if (collapsed.Length > MaxAnchorLength)
        {
            var cut = char.IsHighSurrogate(collapsed[MaxAnchorLength - 1]) ? MaxAnchorLength - 1 : MaxAnchorLength;
            collapsed = collapsed[..cut].TrimEnd();
        }

        return collapsed.Length == 0 ? null : collapsed;
    }

    [GeneratedRegex(@"\s+")]
    private static partial Regex WhiteSpace();
}
