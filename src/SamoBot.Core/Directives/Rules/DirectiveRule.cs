namespace SamoBot.Core.Directives.Rules;

public abstract class DirectiveRule(params string[] names) : IPagePolicyRule
{
    public PagePolicy Apply(PagePolicy policy, PageContext page, IReadOnlyList<RobotsDirective> directives) =>
        directives.Where(directive => names.Any(directive.Is)).Aggregate(policy, Apply);

    protected abstract PagePolicy Apply(PagePolicy policy, RobotsDirective directive);
}
