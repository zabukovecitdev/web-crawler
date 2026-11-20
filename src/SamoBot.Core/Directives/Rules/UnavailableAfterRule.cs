namespace SamoBot.Core.Directives.Rules;

public sealed class UnavailableAfterRule() : DirectiveRule("unavailable_after")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) =>
        DirectiveDate.TryParse(directive.Value, out var until) ? policy.WithIndexUntil(until) : policy;
}
