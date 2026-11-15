namespace SamoBot.Core.Directives.Canonical;

/// <summary>&lt;link rel="canonical" href="..."&gt;, resolved against the base URL of the page.</summary>
public sealed class HtmlCanonicalSource : ICanonicalSource
{
    public Uri? Find(PageContext page) => page.Resolve(page.Page.Canonical);
}
