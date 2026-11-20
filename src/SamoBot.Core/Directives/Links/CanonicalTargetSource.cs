using SamoBot.Core.Directives.Rules;

namespace SamoBot.Core.Directives.Links;

public sealed class CanonicalTargetSource : IRedirectSource
{
    public IEnumerable<Uri> Targets(PageContext page) =>
        NonCanonicalPageRule.IsCopy(page) ? [page.Canonical!] : [];
}
