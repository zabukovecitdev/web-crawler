using SamoBot.Core.Domain;

namespace SamoBot.Core.Scope;

public static class PathTreeBuilder
{
    public const int MaxChildren = 40;

    public static IReadOnlyList<TreeNode> Build(string parent, IReadOnlyList<FolderGroup> groups, int depth)
    {
        var nodes = groups
            .GroupBy(group => group.First, StringComparer.Ordinal)
            .Select(first => Top(parent, first.Key, first.ToList(), depth))
            .OrderBy(node => node.Prefix, StringComparer.Ordinal)
            .ToList();
        return Limit(nodes, parent);
    }

    private static TreeNode Top(string parent, string segment, IReadOnlyList<FolderGroup> groups, int depth)
    {
        var prefix = parent + segment;
        var total = Sum(groups);
        if (!segment.EndsWith('/'))
        {
            return new TreeNode(prefix, TreeNodeKind.Page, false, false, total, null);
        }

        var children = depth > 1 ? Limit(groups.Select(group => Second(prefix, group)).OrderBy(node => node.Prefix, StringComparer.Ordinal).ToList(), prefix) : null;
        return new TreeNode(prefix, TreeNodeKind.Folder, true, false, total, children);
    }

    private static TreeNode Second(string folder, FolderGroup group)
    {
        var prefix = folder + group.Second;
        var isFolder = group.Second.EndsWith('/');
        return new TreeNode(prefix, isFolder ? TreeNodeKind.Folder : TreeNodeKind.Page, isFolder, false, group.Counts, null);
    }

    private static List<TreeNode> Limit(List<TreeNode> nodes, string parent)
    {
        if (nodes.Count <= MaxChildren)
        {
            return nodes;
        }

        var kept = nodes.OrderByDescending(node => node.Counts.Pages).ThenBy(node => node.Prefix, StringComparer.Ordinal).Take(MaxChildren).ToList();
        var rest = nodes.Except(kept).Aggregate(FolderCounts.None, (sum, node) => sum.Plus(node.Counts));
        var restNode = new TreeNode(parent, TreeNodeKind.Rest, false, false, rest, null);
        return kept.OrderBy(node => node.Prefix, StringComparer.Ordinal).Append(restNode).ToList();
    }

    private static FolderCounts Sum(IEnumerable<FolderGroup> groups) =>
        groups.Aggregate(FolderCounts.None, (sum, group) => sum.Plus(group.Counts));
}
