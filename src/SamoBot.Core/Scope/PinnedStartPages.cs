using SamoBot.Core.Domain;

namespace SamoBot.Core.Scope;

public static class PinnedStartPages
{
    public static IReadOnlyList<PathRule> Apply(IEnumerable<PathRule> rules, IEnumerable<string> seedPaths)
    {
        var pinned = seedPaths
            .Select(path => new PathRule(PathRuleKind.Include, path, Exact: true))
            .Distinct()
            .ToList();
        return rules
            .Where(rule => !pinned.Any(pin => pin.Exact == rule.Exact && pin.Prefix == rule.Prefix))
            .Concat(pinned)
            .ToList();
    }
}
