namespace SamoBot.Core.Directives.Canonical;

public sealed class HtmlCanonicalSource : ICanonicalSource
{
    public Uri? Find(PageContext page) => page.Resolve(page.Page.Canonical);
}
