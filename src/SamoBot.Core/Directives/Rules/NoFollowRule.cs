namespace SamoBot.Core.Directives.Rules;

/// <summary>nofollow and none: no link of the page is queued. Redirect targets (meta refresh, canonical) still are.</summary>
public sealed class NoFollowRule() : DirectiveRule("nofollow", "none")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy with { FollowLinks = false };
}
