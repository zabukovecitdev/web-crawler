namespace SamoBot.Core.Directives.Rules;

/// <summary>noarchive: the stored copy of the page is kept for parsing but never shown.</summary>
public sealed class NoArchiveRule() : DirectiveRule("noarchive")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy with { Archive = false };
}
