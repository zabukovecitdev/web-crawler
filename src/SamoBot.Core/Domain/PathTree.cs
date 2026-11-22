using System.Text.Json.Serialization;

namespace SamoBot.Core.Domain;

[JsonConverter(typeof(JsonStringEnumConverter))]
public enum TreeNodeKind
{
    Folder,
    Page,
    Rest
}

public sealed record FolderCounts(
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
    public static readonly FolderCounts None = new(0, 0, 0, 0, 0, 0, 0, null, null);

    public FolderCounts Plus(FolderCounts other) => new(
        Pages + other.Pages,
        Done + other.Done,
        Failed + other.Failed,
        Skipped + other.Skipped,
        Stopped + other.Stopped,
        Fetching + other.Fetching,
        Queued + other.Queued,
        Earliest(OldestModified, other.OldestModified),
        Latest(NewestModified, other.NewestModified));

    private static DateTimeOffset? Earliest(DateTimeOffset? left, DateTimeOffset? right) => new[] { left, right }.Min();

    private static DateTimeOffset? Latest(DateTimeOffset? left, DateTimeOffset? right) => new[] { left, right }.Max();
}

public sealed record FolderGroup(string First, string Second, FolderCounts Counts);

public sealed record TreeNode(
    string Prefix,
    TreeNodeKind Kind,
    bool HasChildren,
    bool Blocked,
    FolderCounts Counts,
    IReadOnlyList<TreeNode>? Children);

public sealed record PathTree(string Host, string Parent, FolderCounts Total, IReadOnlyList<TreeNode> Nodes);
