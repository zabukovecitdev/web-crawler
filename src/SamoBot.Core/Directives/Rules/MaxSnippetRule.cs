using System.Globalization;

namespace SamoBot.Core.Directives.Rules;

public sealed class MaxSnippetRule() : DirectiveRule("max-snippet")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) =>
        int.TryParse(directive.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var limit) && limit >= 0
            ? policy.WithSnippetLimit(limit)
            : policy;
}
