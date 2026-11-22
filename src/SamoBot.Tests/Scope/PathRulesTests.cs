using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Scope;

namespace SamoBot.Tests.Scope;

public class PathRulesTests
{
    private static PathRule Exclude(string prefix, bool exact = false) => new(PathRuleKind.Exclude, prefix, exact);

    private static PathRule Include(string prefix, bool exact = false) => new(PathRuleKind.Include, prefix, exact);

    [Fact]
    public void WithoutRulesEveryPathIsAllowed() => PathRules.None.Allows("/anything").Should().BeTrue();

    [Theory]
    [InlineData("/blog/2016/post", false)]
    [InlineData("/blog/2016/", false)]
    [InlineData("/blog/2016", false)]
    [InlineData("/blog/2017/post", true)]
    [InlineData("/blog/20160/post", true)]
    [InlineData("/blog/", true)]
    public void AFolderRuleCoversTheFolderAndEverythingBelowIt(string path, bool allowed) =>
        new PathRules([Exclude("/blog/2016/")]).Allows(path).Should().Be(allowed);

    [Fact]
    public void AnExactRuleCoversOnlyThatPath()
    {
        var rules = new PathRules([Exclude("/search", exact: true)]);

        rules.Allows("/search").Should().BeFalse();
        rules.Allows("/search/more").Should().BeTrue();
        rules.Allows("/searching").Should().BeTrue();
    }

    [Fact]
    public void TheLongestPrefixWins()
    {
        var rules = new PathRules([Exclude("/blog/"), Include("/blog/2026/"), Exclude("/blog/2026/drafts/")]);

        rules.Allows("/blog/2020/x").Should().BeFalse();
        rules.Allows("/blog/2026/x").Should().BeTrue();
        rules.Allows("/blog/2026/drafts/x").Should().BeFalse();
    }

    [Fact]
    public void AnIncludeBeatsAnExcludeOfTheSamePrefix() =>
        new PathRules([Exclude("/a/"), Include("/a/")]).Allows("/a/b").Should().BeTrue();

    [Fact]
    public void AnExactRuleBeatsAFolderRuleThatIsLonger()
    {
        var rules = new PathRules([Exclude("/guide/install/"), Include("/guide/install", exact: true)]);

        rules.Allows("/guide/install").Should().BeTrue();
        rules.Allows("/guide/install/x").Should().BeFalse();
    }

    [Fact]
    public void TheRootFolderRuleCoversEverything()
    {
        var rules = new PathRules([Exclude("/"), Include("/guide/")]);

        rules.Allows("/blog/x").Should().BeFalse();
        rules.Allows("/guide/x").Should().BeTrue();
        rules.Allows("/").Should().BeFalse();
    }

    [Fact]
    public void AllowsReadsThePathOfAUriAndIgnoresItsQuery() =>
        new PathRules([Exclude("/a/")]).Allows(new Uri("https://x.example/a/b?c=d")).Should().BeFalse();

    [Fact]
    public void PathsAreMatchedWithCase() =>
        new PathRules([Exclude("/Blog/")]).Allows("/blog/x").Should().BeTrue();
}
