namespace SamoBot.Core.SiteChecks;

public static class RobotsFacts
{
    private const string Agent = "user-agent:";
    private const string Disallow = "disallow:";
    private static readonly string[] Ours = ["*", "samobot"];

    public static IReadOnlyList<string> Disallowed(string content) =>
        Groups(content.Split('\n').Select(Clean).Where(line => line.Length > 0))
            .Where(group => group.Agents.Any(IsOurs))
            .SelectMany(group => group.Blocked)
            .Where(path => path.Length > 0)
            .Distinct(StringComparer.Ordinal)
            .ToList();

    private static List<Group> Groups(IEnumerable<string> lines)
    {
        var groups = new List<Group> { new() };
        foreach (var line in lines)
        {
            var isAgent = line.StartsWith(Agent, StringComparison.OrdinalIgnoreCase);
            if (isAgent && groups[^1].HasRules)
            {
                groups.Add(new Group());
            }

            groups[^1].Add(line, isAgent);
        }

        return groups;
    }

    private static bool IsOurs(string agent) => Ours.Contains(agent, StringComparer.OrdinalIgnoreCase);

    private static string Clean(string line)
    {
        var hash = line.IndexOf('#', StringComparison.Ordinal);
        return (hash >= 0 ? line[..hash] : line).Trim();
    }

    private sealed class Group
    {
        public List<string> Agents { get; } = [];
        public List<string> Blocked { get; } = [];
        public bool HasRules { get; private set; }

        public void Add(string line, bool isAgent)
        {
            if (isAgent)
            {
                Agents.Add(line[Agent.Length..].Trim());
                return;
            }

            HasRules = true;
            if (line.StartsWith(Disallow, StringComparison.OrdinalIgnoreCase))
            {
                Blocked.Add(line[Disallow.Length..].Trim());
            }
        }
    }
}
