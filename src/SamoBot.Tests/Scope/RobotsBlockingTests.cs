using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Scope;
using SamoBot.Tests.Fakes;

namespace SamoBot.Tests.Scope;

public class RobotsBlockingTests
{
    private static readonly RobotsRules Rules = new("a.example", "User-agent: *\nDisallow: /admin/\n", TestData.Now, TestData.Now.AddDays(1));

    private static TreeNode Node(string prefix, TreeNodeKind kind = TreeNodeKind.Folder, params TreeNode[] children) =>
        new(prefix, kind, kind == TreeNodeKind.Folder, false, FolderCounts.None, children.Length == 0 ? null : children);

    [Fact]
    public void AFolderThatRobotsTxtDisallowsIsMarkedBlockedWithItsChildren()
    {
        var marked = RobotsBlocking.Mark(Node("/admin/", children: Node("/admin/users", TreeNodeKind.Page)), Rules);

        (marked.Blocked, marked.Children![0].Blocked).Should().Be((true, true));
    }

    [Fact]
    public void AnAllowedFolderIsNotBlocked() => RobotsBlocking.Mark(Node("/guide/"), Rules).Blocked.Should().BeFalse();

    [Fact]
    public void ARestNodeIsNeverBlocked() => RobotsBlocking.Mark(Node("/admin/", TreeNodeKind.Rest), Rules).Blocked.Should().BeFalse();
}
