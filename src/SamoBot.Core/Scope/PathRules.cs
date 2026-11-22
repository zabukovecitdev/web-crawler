using SamoBot.Core.Domain;

namespace SamoBot.Core.Scope;

public sealed class PathRules(IEnumerable<PathRule> rules)
{
    public static readonly PathRules None = new([]);

    private readonly IReadOnlyList<PathRule> _rules = rules.ToList();

    public IReadOnlyList<PathRule> Rules => _rules;

    public bool Allows(Uri uri) => Allows(uri.AbsolutePath);

    public bool Allows(string path)
    {
        var best = _rules
            .Where(rule => Covers(rule, path))
            .OrderByDescending(rule => rule.Exact)
            .ThenByDescending(rule => rule.Prefix.Length)
            .ThenByDescending(rule => rule.Kind == PathRuleKind.Include)
            .FirstOrDefault();
        return best is null || best.Kind == PathRuleKind.Include;
    }

    public static bool Covers(PathRule rule, string path)
    {
        if (rule.Exact)
        {
            return string.Equals(path, rule.Prefix, StringComparison.Ordinal);
        }

        var folder = path.EndsWith('/') ? path : path + "/";
        return folder.StartsWith(rule.Prefix, StringComparison.Ordinal);
    }
}
