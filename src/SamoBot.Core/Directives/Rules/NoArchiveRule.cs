namespace SamoBot.Core.Directives.Rules;

public sealed class NoArchiveRule() : DirectiveRule("noarchive")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy with { Archive = false };
}
