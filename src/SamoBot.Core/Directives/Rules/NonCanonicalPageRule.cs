using SamoBot.Core.Parsing;

namespace SamoBot.Core.Directives.Rules;

/// <summary>
/// A page whose canonical URL is another page is a copy: it is not indexed, so search shows the main version only. The
/// canonical URL itself is queued by <see cref="Links.CanonicalTargetSource"/>.
/// </summary>
public sealed class NonCanonicalPageRule : IPagePolicyRule
{
    public PagePolicy Apply(PagePolicy policy, PageContext page, IReadOnlyList<RobotsDirective> directives) =>
        IsCopy(page) ? policy with { Index = false } : policy;

    public static bool IsCopy(PageContext page) =>
        page.Canonical is { } canonical
        && LinkResolver.Normalize(canonical) is { } target
        && target != LinkResolver.Normalize(page.PageUrl);
}
