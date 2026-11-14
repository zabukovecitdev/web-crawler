using FluentAssertions;
using SamoBot.Core.Parsing;

namespace SamoBot.Tests.Parsing;

public class XRobotsTagTests
{
    [Theory]
    [InlineData("nofollow")]
    [InlineData("NoFollow")]
    [InlineData("noindex, nofollow")]
    [InlineData("none")]
    [InlineData("samobot: nofollow")]
    [InlineData("SamoBot: noindex, nofollow")]
    [InlineData("noarchive\nnofollow")]
    [InlineData("googlebot: noindex\nnofollow")]
    [InlineData("googlebot: noindex, samobot: nofollow")]
    [InlineData("unavailable_after: 25 Jun 2010 15:00:00 PST, nofollow")]
    public void ForbidsFollowForUsOrEveryCrawler(string header) =>
        XRobotsTag.ForbidsFollow(header, "SamoBot").Should().BeTrue();

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("noindex")]
    [InlineData("noindex, noarchive")]
    [InlineData("googlebot: nofollow")]
    [InlineData("googlebot: noindex, nofollow")]
    [InlineData("otherbot: none\nnoindex")]
    [InlineData("nofollowing")]
    [InlineData("max-snippet: 20")]
    public void AllowsFollowOtherwise(string? header) =>
        XRobotsTag.ForbidsFollow(header, "SamoBot").Should().BeFalse();

    [Fact]
    public void AValueWithAColonIsNotACrawlerName() =>
        XRobotsTag.Directives("max-snippet: 20, nofollow", "SamoBot").Should().Equal("max-snippet: 20", "nofollow");

    [Fact]
    public void JoinKeepsOneValuePerLineAndGivesNullForNone()
    {
        XRobotsTag.Join(["noindex", " googlebot: nofollow ", ""]).Should().Be("noindex\ngooglebot: nofollow");
        XRobotsTag.Join([]).Should().BeNull();
        XRobotsTag.Join(null).Should().BeNull();
    }
}
