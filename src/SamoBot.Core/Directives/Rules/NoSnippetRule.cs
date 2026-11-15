namespace SamoBot.Core.Directives.Rules;

/// <summary>nosnippet: search results show the page without a text excerpt.</summary>
public sealed class NoSnippetRule() : DirectiveRule("nosnippet")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy.WithSnippetLimit(0);
}
