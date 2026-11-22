using FluentAssertions;
using SamoBot.Core.SiteChecks;

namespace SamoBot.Tests.SiteChecks;

public class RobotsFactsTests
{
    [Fact]
    public void ListsTheDisallowedPathsOfTheStarGroup() =>
        RobotsFacts.Disallowed("User-agent: *\nDisallow: /admin/\nDisallow: /tmp # old\nAllow: /\n")
            .Should().Equal("/admin/", "/tmp");

    [Fact]
    public void IgnoresGroupsOfOtherCrawlers() =>
        RobotsFacts.Disallowed("User-agent: googlebot\nDisallow: /g/\n\nUser-agent: *\nDisallow: /all/\n")
            .Should().Equal("/all/");

    [Fact]
    public void GroupsOurNameWithTheStarWhenTheyShareRules() =>
        RobotsFacts.Disallowed("User-agent: samobot\nUser-agent: other\nDisallow: /ours/\n").Should().Equal("/ours/");

    [Fact]
    public void AnEmptyDisallowAllowsEverything() =>
        RobotsFacts.Disallowed("User-agent: *\nDisallow:\n").Should().BeEmpty();

    [Fact]
    public void AgentNamesAreMatchedWithoutCase() =>
        RobotsFacts.Disallowed("USER-AGENT: SamoBot\nDISALLOW: /x/\n").Should().Equal("/x/");

    [Fact]
    public void AListedPathAppearsOnce() =>
        RobotsFacts.Disallowed("User-agent: *\nDisallow: /a/\nUser-agent: samobot\nDisallow: /a/\n").Should().Equal("/a/");

    [Fact]
    public void EmptyContentHasNothing() => RobotsFacts.Disallowed(string.Empty).Should().BeEmpty();
}
