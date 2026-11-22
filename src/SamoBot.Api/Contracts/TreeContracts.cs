using SamoBot.Core.Domain;

namespace SamoBot.Api.Contracts;

public sealed record TreeCountsDto(
    int Pages,
    int Done,
    int Failed,
    int Skipped,
    int Stopped,
    int Fetching,
    int Queued,
    DateTimeOffset? OldestModified,
    DateTimeOffset? NewestModified)
{
    public static TreeCountsDto From(FolderCounts counts) => new(
        counts.Pages,
        counts.Done,
        counts.Failed,
        counts.Skipped,
        counts.Stopped,
        counts.Fetching,
        counts.Queued,
        counts.OldestModified,
        counts.NewestModified);
}

public sealed record TreeNodeDto(
    string Prefix,
    TreeNodeKind Kind,
    bool HasChildren,
    bool Blocked,
    TreeCountsDto Counts,
    IReadOnlyList<TreeNodeDto>? Children)
{
    public static TreeNodeDto From(TreeNode node) => new(
        node.Prefix,
        node.Kind,
        node.HasChildren,
        node.Blocked,
        TreeCountsDto.From(node.Counts),
        node.Children?.Select(From).ToList());
}

public sealed record PathTreeDto(string Host, string Parent, TreeCountsDto Total, IReadOnlyList<TreeNodeDto> Nodes)
{
    public static PathTreeDto From(PathTree tree) => new(
        tree.Host,
        tree.Parent,
        TreeCountsDto.From(tree.Total),
        tree.Nodes.Select(TreeNodeDto.From).ToList());
}
