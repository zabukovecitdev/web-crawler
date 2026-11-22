using SamoBot.Core.Domain;

namespace SamoBot.Core.Scope;

public static class TreeParent
{
    public static IReadOnlyList<string> Validate(string parent) =>
        PathRuleValidator.Validate([new PathRule(PathRuleKind.Include, parent)]);
}
