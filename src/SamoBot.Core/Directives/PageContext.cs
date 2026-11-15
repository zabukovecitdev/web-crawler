using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives;

/// <summary>
/// Everything the directive sources and rules may look at for one parsed page: the fetch (with its kept response headers),
/// the parsed page, the page URL, the base URL that relative links resolve against (the page URL, or a valid
/// &lt;base href&gt;), and the canonical URL when the page names one.
/// </summary>
public sealed record PageContext(FetchToParse Fetch, ParsedPage Page, Uri PageUrl, Uri BaseUrl, Uri? Canonical = null)
{
    public ResponseHeaders Headers => Fetch.ResponseHeaders;

    /// <summary>The meta tags with this name attribute (case-insensitive).</summary>
    public IEnumerable<MetaTag> MetaNamed(string name) =>
        Page.MetaTags.Where(tag => !tag.HttpEquiv && tag.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    /// <summary>The meta tags with this http-equiv attribute (case-insensitive).</summary>
    public IEnumerable<MetaTag> HttpEquiv(string name) =>
        Page.MetaTags.Where(tag => tag.HttpEquiv && tag.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    /// <summary>An absolute http or https URL for an href found in the page, resolved against the base URL. Null when it is not one.</summary>
    public Uri? Resolve(string? href) => Resolve(BaseUrl, href);

    internal static Uri? Resolve(Uri against, string? href)
    {
        if (string.IsNullOrWhiteSpace(href) || !Uri.TryCreate(against, href.Trim(), out var absolute))
        {
            return null;
        }

        return absolute.Scheme is "http" or "https" ? absolute : null;
    }
}
