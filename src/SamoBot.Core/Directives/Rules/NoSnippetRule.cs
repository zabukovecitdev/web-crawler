namespace SamoBot.Core.Directives.Rules;

public sealed class NoSnippetRule() : DirectiveRule("nosnippet")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy.WithSnippetLimit(0);
}
