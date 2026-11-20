using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives;

public sealed class PageContextFactory(IEnumerable<ICanonicalSource> canonicalSources)
{
    private readonly IReadOnlyList<ICanonicalSource> _canonicalSources = canonicalSources.ToList();

    public PageContext Create(FetchToParse fetch, ParsedPage page)
    {
        var pageUrl = new Uri(fetch.Url);
        var context = new PageContext(fetch, page, pageUrl, BaseUrl(pageUrl, page.BaseHref));
        return context with { Canonical = _canonicalSources.Select(source => source.Find(context)).FirstOrDefault(uri => uri is not null) };
    }

    private static Uri BaseUrl(Uri pageUrl, string baseHref) => PageContext.Resolve(pageUrl, baseHref) ?? pageUrl;
}
