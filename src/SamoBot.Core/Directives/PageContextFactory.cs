using SamoBot.Core.Domain;

namespace SamoBot.Core.Directives;

/// <summary>Builds the <see cref="PageContext"/> of a parsed page: its base URL first, then its canonical URL.</summary>
public sealed class PageContextFactory(IEnumerable<ICanonicalSource> canonicalSources)
{
    private readonly IReadOnlyList<ICanonicalSource> _canonicalSources = canonicalSources.ToList();

    public PageContext Create(FetchToParse fetch, ParsedPage page)
    {
        var pageUrl = new Uri(fetch.Url);
        var context = new PageContext(fetch, page, pageUrl, BaseUrl(pageUrl, page.BaseHref));
        return context with { Canonical = _canonicalSources.Select(source => source.Find(context)).FirstOrDefault(uri => uri is not null) };
    }

    /// <summary>A &lt;base href&gt; changes what relative links resolve against. It is itself relative to the page URL.</summary>
    private static Uri BaseUrl(Uri pageUrl, string baseHref) => PageContext.Resolve(pageUrl, baseHref) ?? pageUrl;
}
