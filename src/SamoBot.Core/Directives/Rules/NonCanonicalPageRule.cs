using SamoBot.Core.Parsing;

namespace SamoBot.Core.Directives.Rules;

public sealed class NonCanonicalPageRule : IPagePolicyRule
{
    public PagePolicy Apply(PagePolicy policy, PageContext page, IReadOnlyList<RobotsDirective> directives) =>
        IsCopy(page) ? policy with { Index = false } : policy;

    public static bool IsCopy(PageContext page) =>
        page.Canonical is { } canonical
        && LinkResolver.Normalize(canonical) is { } target
        && target != LinkResolver.Normalize(page.PageUrl);
}
