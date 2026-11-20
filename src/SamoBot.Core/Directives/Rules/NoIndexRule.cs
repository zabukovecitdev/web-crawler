namespace SamoBot.Core.Directives.Rules;

public sealed class NoIndexRule() : DirectiveRule("noindex", "none")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy with { Index = false };
}
