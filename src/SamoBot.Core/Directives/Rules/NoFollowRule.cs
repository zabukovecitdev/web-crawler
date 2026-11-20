namespace SamoBot.Core.Directives.Rules;

public sealed class NoFollowRule() : DirectiveRule("nofollow", "none")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) => policy with { FollowLinks = false };
}
