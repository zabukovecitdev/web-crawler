using System.Text.RegularExpressions;
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

    public static IReadOnlyList<PageLink> Prepare(Uri pageUrl, IEnumerable<ParsedLink> links)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<PageLink>();
        foreach (var link in links)
        {
            if (result.Count >= MaxLinksPerPage)
            {
                break;
            }

            var prepared = Build(pageUrl, link);
            if (prepared is not null && seen.Add(prepared.ToUrl))
            {
                result.Add(prepared);
            }
        }

        return result;
    }

    private static PageLink? Build(Uri pageUrl, ParsedLink link)
    {
        if (link.Url.AsSpan().TrimStart().StartsWith("#"))
        {
            return null;
        }

        var target = LinkResolver.ResolveOne(pageUrl, link.Url);
        if (target is null || target.Length > MaxUrlLength)
        {
            return null;
        }

        var host = new Uri(target).Authority;
        return new PageLink(target, host, Anchor(link.LinkText), string.Equals(host, pageUrl.Authority, StringComparison.OrdinalIgnoreCase), link.IsNoFollow);
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
