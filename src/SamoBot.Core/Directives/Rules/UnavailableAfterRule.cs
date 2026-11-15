namespace SamoBot.Core.Directives.Rules;

/// <summary>unavailable_after: DATE: the page leaves the search index at that time. A date that cannot be read is ignored.</summary>
public sealed class UnavailableAfterRule() : DirectiveRule("unavailable_after")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) =>
        DirectiveDate.TryParse(directive.Value, out var until) ? policy.WithIndexUntil(until) : policy;
}
