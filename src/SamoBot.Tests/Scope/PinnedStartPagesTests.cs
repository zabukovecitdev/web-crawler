using FluentAssertions;
using SamoBot.Core.Domain;
using SamoBot.Core.Scope;

namespace SamoBot.Tests.Scope;

public class PinnedStartPagesTests
{
    [Fact]
    public void TheStartPageGetsAnExactIncludeRuleThatBeatsEveryExclude()
    {
        var rules = PinnedStartPages.Apply([new PathRule(PathRuleKind.Exclude, "/")], ["/guide/install"]);

        new PathRules(rules).Allows("/guide/install").Should().BeTrue();
        new PathRules(rules).Allows("/guide/other").Should().BeFalse();
    }

    [Fact]
    public void AnExcludeOfTheStartPageItselfIsReplaced()
    {
        var rules = PinnedStartPages.Apply([new PathRule(PathRuleKind.Exclude, "/guide/install", true)], ["/guide/install"]);

        rules.Should().Equal(new PathRule(PathRuleKind.Include, "/guide/install", true));
    }

    [Fact]
    public void ApplyingTwiceChangesNothing()
    {
        var once = PinnedStartPages.Apply([], ["/"]);

        PinnedStartPages.Apply(once, ["/"]).Should().Equal(once);
    }

    [Fact]
    public void ARootStartPageDoesNotIncludeTheWholeSite()
    {
        var rules = PinnedStartPages.Apply([new PathRule(PathRuleKind.Exclude, "/")], ["/"]);

        new PathRules(rules).Allows("/").Should().BeTrue();
        new PathRules(rules).Allows("/x").Should().BeFalse();
    }
}
