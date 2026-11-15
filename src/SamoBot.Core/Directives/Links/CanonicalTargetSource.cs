using SamoBot.Core.Directives.Rules;

namespace SamoBot.Core.Directives.Links;

/// <summary>A page that names another page as canonical points to the main version, which is queued so it gets crawled.</summary>
public sealed class CanonicalTargetSource : IRedirectSource
{
    public IEnumerable<Uri> Targets(PageContext page) =>
        NonCanonicalPageRule.IsCopy(page) ? [page.Canonical!] : [];
}
