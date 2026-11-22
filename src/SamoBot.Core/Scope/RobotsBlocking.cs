using SamoBot.Core.Crawling;
using SamoBot.Core.Domain;

namespace SamoBot.Core.Scope;

public static class RobotsBlocking
{
    public static TreeNode Mark(TreeNode node, RobotsRules rules)
    {
        var blocked = node.Kind != TreeNodeKind.Rest && !rules.IsAllowed(node.Prefix, RobotsGate.UserAgent);
        return node with { Blocked = blocked, Children = node.Children?.Select(child => Mark(child, rules)).ToList() };
    }
}
