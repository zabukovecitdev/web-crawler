namespace SamoBot.Core.Directives;

public sealed class PagePolicyEvaluator(IEnumerable<IRobotsDirectiveSource> sources, IEnumerable<IPagePolicyRule> rules)
{
    private readonly IReadOnlyList<IRobotsDirectiveSource> _sources = sources.ToList();
    private readonly IReadOnlyList<IPagePolicyRule> _rules = rules.ToList();

    public PagePolicy Evaluate(PageContext page)
    {
        var directives = _sources.SelectMany(source => source.Read(page)).ToList();
        var initial = PagePolicy.Default with { Directives = directives.Select(d => d.ToString()).Distinct().ToList() };
        return _rules.Aggregate(initial, (policy, rule) => rule.Apply(policy, page, directives));
    }
}
