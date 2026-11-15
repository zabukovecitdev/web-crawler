using System.Globalization;

namespace SamoBot.Core.Directives.Rules;

/// <summary>max-snippet: N limits the excerpt to N characters; 0 means no excerpt; -1 or an invalid value means no limit.</summary>
public sealed class MaxSnippetRule() : DirectiveRule("max-snippet")
{
    protected override PagePolicy Apply(PagePolicy policy, RobotsDirective directive) =>
        int.TryParse(directive.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var limit) && limit >= 0
            ? policy.WithSnippetLimit(limit)
            : policy;
}
