namespace SamoBot.Core.Directives.Rules;

/// <summary>noindex and none: the page is kept out of the search index, and removed from it if it was indexed before.</summary>
public sealed class NoIndexRule() : DirectiveRule("noindex", "none")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy with { Index = false };
}
