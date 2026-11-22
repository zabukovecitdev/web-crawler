using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Scope;

namespace SamoBot.Tests.Scope;

public class PathTreeBuilderTests
{
    private static FolderGroup Group(string first, string second, int pages) =>
        new(first, second, FolderCounts.None with { Pages = pages });

    [Fact]
    public void TopLevelFoldersAndPagesComeBackSortedWithTheirTotals()
    {
        var nodes = PathTreeBuilder.Build("/", [Group("guide/", "install", 1), Group("guide/", "quickstart", 1), Group("blog/", "2026/", 64), Group("changelog", "", 37)], 1);

        nodes.Select(node => (node.Prefix, node.Kind, node.Counts.Pages)).Should().Equal(
            ("/blog/", TreeNodeKind.Folder, 64),
            ("/changelog", TreeNodeKind.Page, 37),
            ("/guide/", TreeNodeKind.Folder, 2));
        nodes.Should().OnlyContain(node => node.Children == null);
    }

    [Fact]
    public void ADepthOfTwoAddsTheNextLevelInsideEachFolder()
    {
        var nodes = PathTreeBuilder.Build("/", [Group("guide/", "", 1), Group("guide/", "install", 1), Group("guide/", "advanced/", 40)], 2);

        var guide = nodes.Should().ContainSingle().Subject;
        guide.Counts.Pages.Should().Be(42);
        guide.Children!.Select(child => (child.Prefix, child.Kind, child.HasChildren)).Should().Equal(
            ("/guide/", TreeNodeKind.Page, false),
            ("/guide/advanced/", TreeNodeKind.Folder, true),
            ("/guide/install", TreeNodeKind.Page, false));
    }

    [Fact]
    public void ThePageOfTheParentItselfIsAPageNodeWithTheParentPrefix()
    {
        var nodes = PathTreeBuilder.Build("/guide/", [Group("", "", 1), Group("a/", "", 5)], 1);

        nodes.Select(node => (node.Prefix, node.Kind)).Should().Equal(("/guide/", TreeNodeKind.Page), ("/guide/a/", TreeNodeKind.Folder));
    }

    [Fact]
    public void ABigFolderKeepsTheLargestChildrenAndSumsTheRestIntoOneNode()
    {
        var groups = Enumerable.Range(0, PathTreeBuilder.MaxChildren + 10).Select(i => Group($"s{i:D3}/", "", i + 1)).ToList();

        var nodes = PathTreeBuilder.Build("/", groups, 1);

        nodes.Should().HaveCount(PathTreeBuilder.MaxChildren + 1);
        var rest = nodes[^1];
        (rest.Kind, rest.Prefix, rest.Counts.Pages).Should().Be((TreeNodeKind.Rest, "/", Enumerable.Range(1, 10).Sum()));
        nodes.Take(PathTreeBuilder.MaxChildren).Should().BeInAscendingOrder(node => node.Prefix, StringComparer.Ordinal);
    }

    [Fact]
    public void TheCountsOfTheNodesAddUpToTheCountsOfTheGroups()
    {
        var groups = new[] { Group("a/", "x", 3), Group("a/", "y/", 4), Group("b", "", 5) };

        var nodes = PathTreeBuilder.Build("/", groups, 2);

        nodes.Sum(node => node.Counts.Pages).Should().Be(12);
    }

    [Fact]
    public void ModifiedRangesAndRunCountsAreCombined()
    {
        var early = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var late = early.AddMonths(6);
        var groups = new[]
        {
            new FolderGroup("a/", "x", new FolderCounts(1, 1, 0, 0, 0, 0, 0, late, late)),
            new FolderGroup("a/", "y", new FolderCounts(2, 0, 1, 0, 0, 1, 0, early, early.AddDays(1)))
        };

        var folder = PathTreeBuilder.Build("/", groups, 1).Single().Counts;

        (folder.Pages, folder.Done, folder.Failed, folder.Fetching, folder.OldestModified, folder.NewestModified).Should().Be((3, 1, 1, 1, early, late));
    }
}
