using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives;

public sealed record PageContext(FetchToParse Fetch, ParsedPage Page, Uri PageUrl, Uri BaseUrl, Uri? Canonical = null)
{
    public ResponseHeaders Headers => Fetch.ResponseHeaders;

    public IEnumerable<MetaTag> MetaNamed(string name) =>
        Page.MetaTags.Where(tag => !tag.HttpEquiv && tag.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

    public IEnumerable<MetaTag> HttpEquiv(string name) =>
        Page.MetaTags.Where(tag => tag.HttpEquiv && tag.Name.Equals(name, StringComparison.OrdinalIgnoreCase));

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
