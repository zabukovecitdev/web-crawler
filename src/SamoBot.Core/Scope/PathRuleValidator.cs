using SamoBot.Core.Domain;

namespace SamoBot.Core.Scope;

public static class PathRuleValidator
{
    public const int MaxRules = 500;
    public const int MaxPrefixLength = 512;

    public static IReadOnlyList<string> Validate(IReadOnlyList<PathRule>? rules)
    {
        if (rules is null || rules.Count == 0)
        {
            return [];
        }

        var errors = new List<string>();
        if (rules.Count > MaxRules)
        {
            errors.Add($"At most {MaxRules} path rules are allowed.");
        }

        errors.AddRange(rules.Take(MaxRules).Select(Problem).OfType<string>().Distinct());
        return errors;
    }

    private static string? Problem(PathRule rule)
    {
        if (!Enum.IsDefined(rule.Kind))
        {
            return "A path rule is neither include nor exclude.";
        }

        return PrefixProblem(rule.Prefix) ?? (!rule.Exact && !rule.Prefix.EndsWith('/') ? "A folder rule must end with a slash." : null);
    }

    private static string? PrefixProblem(string prefix)
    {
        if (string.IsNullOrEmpty(prefix) || !prefix.StartsWith('/'))
        {
            return "A path rule must start with a slash.";
        }

        if (prefix.Length > MaxPrefixLength)
        {
            return $"A path rule is longer than {MaxPrefixLength} characters.";
        }

        return prefix.Any(IsForbidden) ? "A path rule must not contain a query, a fragment or control characters." : null;
    }

    private static bool IsForbidden(char character) => character is '?' or '#' || char.IsControl(character);
}
